// Скальпер Binance USDT-M Futures. Высокий риск ликвидации и потерь. Только для образования/тестнета.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"binance-scalper/internal/futures"
	"binance-scalper/internal/scanner"

	"github.com/joho/godotenv"
)

const (
	leverage        = 20
	tpPriceMove     = 0.015 // +1.5% цены ≈ +30% PnL при 20x
	slPriceMove     = 0.005 // -0.5% цены ≈ -10% PnL при 20x
	imbLongMin      = 0.6
	imbShortMax     = -0.6
	rsiLongMax      = 30.0
	rsiShortMin     = 70.0
	minTradeGap     = 800 * time.Millisecond
	wsReconnectWait = 2 * time.Second
)

type signalKind int

const (
	sigBuyBurst signalKind = iota
	sigSellBurst
	sigComboLong
	sigComboShort
)

type dash struct {
	mu              sync.Mutex
	symbol          string
	imbalance       float64
	rsi             float64
	volSpike        bool
	mark            float64
	posQty          float64
	entry           float64
	uPnL            float64
	realizedSession float64
	trades          int
	lastSig         string
	lastErr         string
	wsMarketOK      bool
	wsUserOK        bool
}

func (d *dash) render() {
	d.mu.Lock()
	imb, rsi, vs, mk, qty, ent, upnl, rp, tr, sig, err, mOk, uOk := d.imbalance, d.rsi, d.volSpike, d.mark, d.posQty, d.entry, d.uPnL, d.realizedSession, d.trades, d.lastSig, d.lastErr, d.wsMarketOK, d.wsUserOK
	sym := d.symbol
	d.mu.Unlock()

	fmt.Print("\033[H\033[2J")
	fmt.Println("══ Binance Futures Scalper ══", time.Now().Format("15:04:05"))
	fmt.Printf("Symbol %s  |  WS market: %v  user: %v\n", sym, mOk, uOk)
	fmt.Println("────────────────────────────────────────")
	fmt.Printf("Order book imbalance: %+.4f  (long>%.2f short<%.2f)\n", imb, imbLongMin, imbShortMax)
	fmt.Printf("RSI(1m,14):           %.2f  (long<%v short>%v)\n", rsi, rsiLongMax, rsiShortMin)
	fmt.Printf("Volume spike (300%%):  %v\n", vs)
	fmt.Printf("Mark:                 %.6f\n", mk)
	fmt.Println("────────────────────────────────────────")
	side := "FLAT"
	if qty > 0 {
		side = "LONG"
	} else if qty < 0 {
		side = "SHORT"
	}
	fmt.Printf("Position: %s  qty: %.6f  entry: %.6f\n", side, qty, ent)
	fmt.Printf("Unrealized PnL (approx): %.4f USDT\n", upnl)
	fmt.Printf("Session realized PnL:    %.4f USDT\n", rp)
	fmt.Printf("Closed trades (session): %d\n", tr)
	fmt.Println("────────────────────────────────────────")
	fmt.Printf("Last signal: %s\n", sig)
	if err != "" {
		fmt.Printf("Last error:  %s\n", err)
	}
	fmt.Println("Ctrl+C — выход")
}

func parseEnv() (key, secret, symbol string, qty float64) {
	key = strings.TrimSpace(os.Getenv("BINANCE_API_KEY"))
	secret = strings.TrimSpace(os.Getenv("BINANCE_API_SECRET"))
	symbol = strings.TrimSpace(os.Getenv("SYMBOL"))
	if symbol == "" {
		symbol = "SOLUSDT"
	}
	qty = 0.07
	if s := os.Getenv("QTY"); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v > 0 {
			qty = v
		}
	}
	return
}

func backoffSleep(attempt int) {
	d := time.Duration(200*(1<<min(attempt, 6))) * time.Millisecond
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	time.Sleep(d)
}

func withRetry429(ctx context.Context, op func() error) error {
	for attempt := 0; ; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		err := op()
		if err == nil {
			return nil
		}
		var rl *futures.RateLimitError
		if errors.As(err, &rl) {
			backoffSleep(attempt)
			continue
		}
		return err
	}
}

func main() {
	log.SetFlags(0)
	_ = godotenv.Load()

	key, secret, symbol, qty := parseEnv()
	if key == "" || secret == "" {
		log.Fatal("Задайте BINANCE_API_KEY и BINANCE_API_SECRET")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := futures.NewClient(key, secret)

	if err := withRetry429(ctx, func() error {
		return client.SetMarginType(ctx, symbol, "CROSSED")
	}); err != nil {
		log.Fatalf("marginType: %v", err)
	}
	if err := withRetry429(ctx, func() error {
		return client.SetLeverage(ctx, symbol, leverage)
	}); err != nil {
		log.Fatalf("leverage: %v", err)
	}

	closes, err := client.Klines(ctx, symbol, "1m", 60)
	if err != nil {
		log.Fatalf("klines: %v", err)
	}

	sigCh := make(chan signalKind, 32)
	hub := scanner.NewHub(symbol, 120,
		func() {
			select {
			case sigCh <- sigBuyBurst:
			default:
			}
		},
		func() {
			select {
			case sigCh <- sigSellBurst:
			default:
			}
		},
	)
	hub.SeedCloses(closes)

	d := &dash{symbol: symbol}

	var (
		posMu       sync.Mutex
		hasPosition bool
		lastTrade   time.Time
	)

	trySignal := func(kind signalKind) {
		posMu.Lock()
		defer posMu.Unlock()
		if hasPosition {
			return
		}
		if time.Since(lastTrade) < minTradeGap {
			return
		}

		var side string
		var label string
		switch kind {
		case sigBuyBurst:
			side, label = "BUY", "ORDER_FLOW_BUY_BURST"
		case sigSellBurst:
			side, label = "SELL", "ORDER_FLOW_SELL_BURST"
		case sigComboLong:
			side, label = "BUY", "COMBO_LONG"
		case sigComboShort:
			side, label = "SELL", "COMBO_SHORT"
		default:
			return
		}

		e := withRetry429(ctx, func() error {
			_, err := client.MarketOrder(ctx, symbol, side, qty)
			return err
		})
		if e != nil {
			d.mu.Lock()
			d.lastErr = e.Error()
			d.mu.Unlock()
			return
		}
		lastTrade = time.Now()
		hasPosition = true
		d.mu.Lock()
		d.lastSig = label
		d.lastErr = ""
		d.mu.Unlock()

		time.Sleep(400 * time.Millisecond)
		pr, _ := client.PositionRisk(ctx, symbol)
		var entry, amt float64
		for _, p := range pr {
			if p.Symbol != symbol {
				continue
			}
			fmt.Sscanf(p.PositionAmt, "%f", &amt)
			fmt.Sscanf(p.EntryPrice, "%f", &entry)
			break
		}
		if math.Abs(amt) < 1e-12 {
			hasPosition = false
			return
		}
		if amt > 0 {
			_ = withRetry429(ctx, func() error {
				return client.PlaceBracketAfterLong(ctx, symbol, entry, tpPriceMove, slPriceMove)
			})
		} else {
			_ = withRetry429(ctx, func() error {
				return client.PlaceBracketAfterShort(ctx, symbol, entry, tpPriceMove, slPriceMove)
			})
		}
	}

	// скан комбо-сигналов (частота ограничена minTradeGap)
	go func() {
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				imb := hub.Imbalance()
				rsi := hub.RSI()
				vs := hub.VolumeSpike()
				if imb > imbLongMin && rsi < rsiLongMax && vs {
					select {
					case sigCh <- sigComboLong:
					default:
					}
				}
				if imb < imbShortMax && rsi > rsiShortMin && vs {
					select {
					case sigCh <- sigComboShort:
					default:
					}
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case k := <-sigCh:
				trySignal(k)
			}
		}
	}()

	listenKey, err := client.StartUserStream(ctx)
	if err != nil {
		log.Fatalf("listenKey: %v", err)
	}
	go func() {
		t := time.NewTicker(30 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = client.KeepAliveUserStream(context.Background(), listenKey)
			}
		}
	}()

	onOrder := func(raw json.RawMessage) {
		var wrap struct {
			O struct {
				PositionAmt   string `json:"pa"`
				Status        string `json:"X"`
				RP            string `json:"rp"`
				ExecutionType string `json:"x"`
			} `json:"o"`
		}
		if json.Unmarshal(raw, &wrap) != nil {
			return
		}
		var rp float64
		fmt.Sscanf(wrap.O.RP, "%f", &rp)
		if wrap.O.ExecutionType == "TRADE" && rp != 0 {
			d.mu.Lock()
			d.realizedSession += rp
			d.trades++
			d.mu.Unlock()
		}
		if wrap.O.Status == "FILLED" || wrap.O.Status == "PARTIALLY_FILLED" {
			var pa float64
			fmt.Sscanf(wrap.O.PositionAmt, "%f", &pa)
			if math.Abs(pa) < 1e-12 {
				posMu.Lock()
				hasPosition = false
				posMu.Unlock()
			}
		}
	}

	go func() {
		attempt := 0
		for {
			if ctx.Err() != nil {
				return
			}
			err := scanner.RunCombined(ctx, symbol, hub)
			d.mu.Lock()
			d.wsMarketOK = false
			if err != nil && ctx.Err() == nil {
				d.lastErr = "market WS: " + err.Error()
			}
			d.mu.Unlock()
			attempt++
			select {
			case <-ctx.Done():
				return
			case <-time.After(wsReconnectWait):
			}
		}
	}()

	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			err := scanner.RunUserData(ctx, listenKey, onOrder)
			d.mu.Lock()
			d.wsUserOK = false
			if err != nil && ctx.Err() == nil {
				d.lastErr = "user WS: " + err.Error()
			}
			d.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-time.After(wsReconnectWait):
			}
		}
	}()

	// помечаем WS как OK после короткой задержки подключения
	go func() {
		time.Sleep(2 * time.Second)
		d.mu.Lock()
		d.wsMarketOK = true
		d.wsUserOK = true
		d.mu.Unlock()
	}()

	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			d.mu.Lock()
			d.imbalance = hub.Imbalance()
			d.rsi = hub.RSI()
			d.volSpike = hub.VolumeSpike()
			d.mark = hub.MarkPrice()
			d.mu.Unlock()

			if pr, err := client.PositionRisk(ctx, symbol); err == nil {
				for _, p := range pr {
					if p.Symbol != symbol {
						continue
					}
					var pa, ep, upnl float64
					fmt.Sscanf(p.PositionAmt, "%f", &pa)
					fmt.Sscanf(p.EntryPrice, "%f", &ep)
					fmt.Sscanf(p.UnRealizedProfit, "%f", &upnl)
					d.mu.Lock()
					d.posQty = pa
					d.entry = ep
					d.uPnL = upnl
					d.mu.Unlock()
					break
				}
			}
			d.render()
		}
	}
}
