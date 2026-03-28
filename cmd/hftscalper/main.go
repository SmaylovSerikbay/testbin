// HFT-скальпер Binance USDT-M Futures (go-binance/v2). Высокий риск. Не финансовый совет.
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"binance-scalper/internal/hft"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/joho/godotenv"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	_ = godotenv.Load()

	hft.ApplyBinanceEndpoints()

	envTrue := func(k string) bool {
		v := strings.ToLower(strings.TrimSpace(os.Getenv(k)))
		return v == "1" || v == "true" || v == "yes"
	}
	if envTrue("HFT_AGGRESSIVE") {
		hft.ApplyAggressive()
		log.Println("HFT_AGGRESSIVE: очень мягкие пороги (только демо/обучение; не ждите качества)")
	} else if envTrue("DEMO_RELAXED") {
		hft.ApplyDemoRelaxed()
		log.Println("DEMO_RELAXED: пороги стратегии снижены")
	}

	apiKey := strings.TrimSpace(os.Getenv("BINANCE_API_KEY"))
	secret := strings.TrimSpace(os.Getenv("BINANCE_API_SECRET"))
	if apiKey == "" || secret == "" {
		log.Fatal("BINANCE_API_KEY / BINANCE_API_SECRET обязательны")
	}

	// Сколько пар одновременно (на каждую — 2 WS). «Все 400» = сотни соединений и лимиты Binance.
	maxSym := 12
	if s := os.Getenv("UNIVERSE_SIZE"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxSym = n
		}
	}
	qty := strings.TrimSpace(os.Getenv("QTY"))
	if qty == "" {
		qty = "0.07"
	}
	neutralUSDT := 0.25
	if s := os.Getenv("NEUTRAL_EXIT_USDT"); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v > 0 {
			neutralUSDT = v
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := futures.NewClient(apiKey, secret)
	client.SetApiEndpoint(futures.BaseApiMainUrl)

	if err := hft.LoadFuturesRules(ctx, client); err != nil {
		log.Printf("LoadFuturesRules: %v (qty/stop precision могут быть неверными)", err)
	}

	watchEnv := os.Getenv("WATCH")
	symbols, err := hft.ResolveWatchlist(ctx, client, maxSym, watchEnv)
	if err != nil {
		log.Fatalf("watchlist: %v", err)
	}
	if len(symbols) == 0 {
		log.Fatal("пустой список символов")
	}
	log.Printf("Список наблюдения: %d символов (фильтр PERPETUAL+book; сортировка см. UNIVERSE_SORT)", len(symbols))

	for _, sym := range symbols {
		if err := hft.SetupSymbol(ctx, client, sym); err != nil {
			log.Printf("setup %s: %v", sym, err)
		}
	}

	stats := hft.NewSessionStats(16)
	hub := hft.NewPriceHub()

	listenKey, err := client.NewStartUserStreamService().Do(ctx)
	if err != nil {
		log.Fatalf("listenKey: %v", err)
	}
	go func() {
		t := time.NewTicker(25 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = client.NewKeepaliveUserStreamService().ListenKey(listenKey).Do(context.Background())
			}
		}
	}()

	userErr := func(e error) {
		if e != nil && ctx.Err() == nil {
			log.Printf("user WS: %v", e)
		}
	}
	_, stopUser, err := futures.WsUserDataServe(listenKey, func(ev *futures.WsUserDataEvent) {
		if ev == nil || ev.Event != futures.UserDataEventTypeOrderTradeUpdate {
			return
		}
		o := ev.OrderTradeUpdate
		if o.ExecutionType != futures.OrderExecutionTypeTrade || !o.IsReduceOnly {
			return
		}
		rp, _ := strconv.ParseFloat(o.RealizedPnL, 64)
		stats.AddRealized(rp)
	}, userErr)
	if err != nil {
		log.Fatalf("user stream: %v", err)
	}
	go func() {
		<-ctx.Done()
		close(stopUser)
	}()

	go hft.PositionCare(ctx, client, symbols, hub, neutralUSDT)

	hotFlags := make(map[string]*atomic.Bool)
	for _, sym := range symbols {
		hotFlags[sym] = new(atomic.Bool)
	}

	for _, sym := range symbols {
		sym := sym
		w := hft.NewSymbolWorker(sym, client, qty, stats, hub)
		flag := hotFlags[sym]
		go w.Run(ctx, flag)
	}

	if envTrue("DEMO_SMOKE_ORDER") {
		smokeSym := strings.ToUpper(strings.TrimSpace(os.Getenv("SMOKE_SYMBOL")))
		if smokeSym == "" {
			smokeSym = symbols[0]
		}
		log.Printf("DEMO_SMOKE_ORDER: через 5с MARKET LONG %s qty=%s (проверка API)", smokeSym, qty)
		go func(sym string) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			c2, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			if err := hft.MarketOpenLong(c2, client, sym, qty); err != nil {
				stats.SetOrderError("SMOKE: " + err.Error())
				stats.PushImpulse("SMOKE FAIL " + sym + " " + err.Error())
				log.Printf("SMOKE order error: %v", err)
			} else {
				stats.PushImpulse("SMOKE OK " + sym + " LONG")
				log.Printf("SMOKE: ордер отправлен %s", sym)
			}
		}(smokeSym)
	}

	go panicKeyboard(ctx, client, symbols)

	tick := time.NewTicker(900 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			total, winPct, impulses := stats.Snapshot()
			fmt.Print("\033[H\033[2J")
			fmt.Println("══ HFT Futures Scalper (go-binance) ══", time.Now().Format("15:04:05"))
			fmt.Printf("REST: %s  WS: %s\n", futures.BaseApiMainUrl, futures.BaseWsMainUrl)
			if strings.Contains(futures.BaseApiMainUrl, "demo") {
				fmt.Println("(!) Демо API — стаканы/сделки другие, чем на binance.com в браузере (там прод).")
			}
			fmt.Printf("Symbols: %v  Qty: %s  Cross %dx\n", symbols, qty, hft.Leverage)
			fmt.Println("────────────────────────────────────────")
			fmt.Printf("Session realized PnL: %.4f USDT\n", total)
			fmt.Printf("Win rate (reduce-only): %.1f%%  (цель >70%%)\n", winPct)
			fmt.Println("Active impulses (последние):")
			if len(impulses) == 0 {
				fmt.Println("  —")
			} else {
				for i := len(impulses) - 1; i >= 0 && i >= len(impulses)-8; i-- {
					fmt.Println(" •", impulses[i])
				}
			}
			if errTxt := stats.LastOrderError(); errTxt != "" {
				fmt.Println("────────────────────────────────────────")
				fmt.Println("Последняя ошибка ордера / API:")
				fmt.Println(" ", errTxt)
			}
			fmt.Println("────────────────────────────────────────")
			fmt.Println("Market heat (есть цена и «жар» ≤45с):")
			for _, s := range symbols {
				h := hotFlags[s].Load()
				state := "COLD"
				if h {
					state = "HOT"
				}
				px := hub.Get(s)
				if px <= 0 {
					state = "NO_WS"
				}
				fmt.Printf("  %s  %s  px≈%.6f\n", s, state, px)
			}
			fmt.Println("────────────────────────────────────────")
			fmt.Println("Тест ордера: в .env DEMO_SMOKE_ORDER=1 (+ SMOKE_SYMBOL=BTCUSDT) — через 5с MARKET LONG")
			fmt.Println("Волатильные пары: UNIVERSE_SORT=change UNIVERSE_SIZE=20 | чаще сигналы: HFT_AGGRESSIVE=1")
			fmt.Println("Введите PANIC + Enter — закрыть всё; Ctrl+C — выход")
		}
	}
}

func panicKeyboard(ctx context.Context, client *futures.Client, symbols []string) {
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		line := strings.ToUpper(strings.TrimSpace(sc.Text()))
		if line == "PANIC" || line == "P" {
			c, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			_ = hft.PanicCloseAll(c, client, symbols)
			cancel()
			log.Println("PANIC: все позиции закрыты / отменены ордера по списку")
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
