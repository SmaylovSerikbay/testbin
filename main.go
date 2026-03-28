// Сканер USDT-M фьючерсов: мини-тикер ~1 с. Памп = резкий рост цены И всплеск секундного
// котируемого объёма (Δq), плюс порог относительно медианы |1s-рет| по символу (меньше мусора).
// Симуляция: маржа $1, 20×, TP/SL по марже +60% / −15%.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	marginUSDT     = 1.0
	leverage       = 20.0
	tpMarginPct    = 60.0 // тейк-профит в % от маржи
	slMarginPct    = 15.0 // стоп в % от маржи
	feeRoundTrip   = 0.0008 // ~0.04% вход + выход на нотиoнал (оценка)
	minSymbols     = 400
	streamsPerConn = 120 // чанки URL + нагрузка на один сокет
	readDeadLine   = 60 * time.Second

	// «Реальный» памп: нижняя планка по цене, если истории ещё мало
	defaultPumpFloorPct = 0.18
	// Насколько текущий 1s-рет должен превышать медиану |рет| за окно (адаптивно по символу)
	defaultRetMult = 5.0
	// Минимум USDT, прошедших за ~1 с (Δq), и во сколько раз выше медианы секундного потока
	defaultMinQuoteDelta = 80_000.0
	defaultVolMult       = 4.0
	defaultHistLen       = 45
	defaultWarmupBars    = 25
	defaultMaxPumpPct    = 8.0 // выше — скорее баг/аукцион, не считаем
	defaultCooldownSec   = 90
	defaultPumpMinPct    = 0.12 // минимум из env PUMP_PCT_1S
)

type envMap map[string]string

func loadEnv(path string) envMap {
	m := make(envMap)
	f, err := os.Open(path)
	if err != nil {
		return m
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.IndexByte(line, '=')
		if i <= 0 {
			continue
		}
		k, v := strings.TrimSpace(line[:i]), strings.TrimSpace(line[i+1:])
		m[k] = v
	}
	return m
}

func envGet(m envMap, key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	if m != nil {
		if v := strings.TrimSpace(m[key]); v != "" {
			return v
		}
	}
	return def
}

func envGetFloat(m envMap, key string, def float64) float64 {
	s := envGet(m, key, "")
	if s == "" {
		return def
	}
	x, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return def
	}
	return x
}

func envGetInt(m envMap, key string, def int) int {
	s := envGet(m, key, "")
	if s == "" {
		return def
	}
	x, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return x
}

func medianSorted(s []float64) float64 {
	if len(s) == 0 {
		return 0
	}
	sort.Float64s(s)
	mid := len(s) / 2
	if len(s)%2 == 1 {
		return s[mid]
	}
	return (s[mid-1] + s[mid]) / 2
}

// ---- REST ----

type exchSymbol struct {
	Symbol       string `json:"symbol"`
	Status       string `json:"status"`
	ContractType string `json:"contractType"`
	QuoteAsset   string `json:"quoteAsset"`
}

type exchangeInfo struct {
	Symbols []exchSymbol `json:"symbols"`
}

type ticker24 struct {
	Symbol       string `json:"symbol"`
	QuoteVolume  string `json:"quoteVolume"`
	LastPrice    string `json:"lastPrice"`
}

func httpGetJSON(ctx context.Context, client *http.Client, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func fetchUniverse(ctx context.Context, baseREST string, needMin int) ([]string, error) {
	client := &http.Client{Timeout: 25 * time.Second}
	var ei exchangeInfo
	if err := httpGetJSON(ctx, client, baseREST+"/fapi/v1/exchangeInfo", &ei); err != nil {
		return nil, err
	}
	var cands []string
	for _, s := range ei.Symbols {
		if s.Status != "TRADING" || s.QuoteAsset != "USDT" || s.ContractType != "PERPETUAL" {
			continue
		}
		cands = append(cands, s.Symbol)
	}
	if len(cands) == 0 {
		return nil, fmt.Errorf("нет USDT PERPETUAL TRADING символов")
	}
	var tickers []ticker24
	if err := httpGetJSON(ctx, client, baseREST+"/fapi/v1/ticker/24hr", &tickers); err != nil {
		return cands, nil // fallback: без сортировки
	}
	vol := make(map[string]float64, len(tickers))
	for _, t := range tickers {
		v, _ := strconv.ParseFloat(t.QuoteVolume, 64)
		vol[t.Symbol] = v
	}
	sort.Slice(cands, func(i, j int) bool {
		return vol[cands[i]] > vol[cands[j]]
	})
	if len(cands) > needMin {
		cands = cands[:needMin]
	}
	return cands, nil
}

// ---- Состояние по символу ----

type symState struct {
	mu           sync.Mutex
	lastPrice    float64
	haveLast     bool
	lastQuoteVol float64 // накопленный quote volume (24h) с прошлого тика
	haveQuoteVol bool
	inPos        bool
	entry        float64
	qty          float64
	notional     float64
	openedAt     time.Time
	pumpArmCnt   int
	lastSignalAt time.Time

	retBuf   []float64 // кольцо: % изменения цены за тик (~1 с)
	dqBuf    []float64 // кольцо: Δq USDT за тик; 0 = нет данных за эту секунду
	scratch  []float64
	barsSeen int // обработано тиков цены (вне позиции и в момент смены last)
}

func priceMoveForMarginPct(marginPct float64, lev float64) float64 {
	return (marginPct / 100) / lev
}

// ---- WS ----

type miniWrap struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type miniTicker struct {
	E string `json:"e"`
	S string `json:"s"`
	C string `json:"c"` // last price
	Q string `json:"q"` // накопленный quote volume за 24h
}

func parseMiniPayload(raw []byte) (sym string, price float64, quoteVol float64, ok bool) {
	var w miniWrap
	if json.Unmarshal(raw, &w) == nil && len(w.Data) > 0 {
		raw = w.Data
	}
	var t miniTicker
	if err := json.Unmarshal(raw, &t); err != nil {
		return "", 0, 0, false
	}
	if t.S == "" || t.C == "" {
		return "", 0, 0, false
	}
	p, err := strconv.ParseFloat(t.C, 64)
	if err != nil || p <= 0 || math.IsNaN(p) {
		return "", 0, 0, false
	}
	var q float64
	if strings.TrimSpace(t.Q) != "" {
		q, err = strconv.ParseFloat(t.Q, 64)
		if err != nil || q < 0 || math.IsNaN(q) {
			q = -1
		}
	} else {
		q = -1
	}
	return t.S, p, q, true
}

type hub struct {
	states       sync.Map // symbol -> *symState
	pumpPct      float64 // минимальный % движения (планка с env)
	pumpFloorPct float64 // доп. нижняя планка против шума
	pumpTicks    int
	retMult      float64
	minQuoteDlt  float64
	volMult      float64
	warmupBars   int
	maxPumpPct   float64
	cooldown     time.Duration
	histLen      int

	tpPriceFrac  float64
	slPriceFrac  float64
	feeRT        float64
	marginUSDT   float64
	notionalUSDT float64
	totalPnL     atomic.Uint64 // bits of float64
	closedTrades atomic.Uint64
}

func bitsFromFloat64(x float64) uint64 { return math.Float64bits(x) }
func float64FromBits(b uint64) float64 { return math.Float64frombits(b) }

func (h *hub) addPnL(delta float64) {
	for {
		old := h.totalPnL.Load()
		cur := float64FromBits(old) + delta
		if h.totalPnL.CompareAndSwap(old, bitsFromFloat64(cur)) {
			break
		}
	}
}

func (st *symState) medianAbsRet(scratch []float64, histLen int) float64 {
	if histLen <= 0 || len(st.retBuf) < histLen {
		return 0
	}
	scratch = scratch[:0]
	for _, r := range st.retBuf[:histLen] {
		if r < 0 {
			r = -r
		}
		scratch = append(scratch, r)
	}
	if len(scratch) == 0 {
		return 0
	}
	return medianSorted(scratch)
}

func (st *symState) medianDQ(scratch []float64, histLen int) float64 {
	if histLen <= 0 || len(st.dqBuf) < histLen {
		return 0
	}
	scratch = scratch[:0]
	for _, v := range st.dqBuf[:histLen] {
		if v > 0 {
			scratch = append(scratch, v)
		}
	}
	if len(scratch) < histLen/4 { // слишком мало секунд с объёмом — не доверяем
		return 0
	}
	return medianSorted(scratch)
}

func (h *hub) processTick(sym string, price, quoteVol float64, haveQ bool, now time.Time) {
	v, _ := h.states.LoadOrStore(sym, newSymState(h.notionalUSDT, h.histLen))
	st := v.(*symState)

	st.mu.Lock()
	defer st.mu.Unlock()

	if st.inPos {
		if price >= st.entry*h.tpPriceFrac {
			pnl := st.qty*(price-st.entry) - h.feeRT*st.notional
			h.closeTrade(st, sym, "TP", price, pnl, now)
			return
		}
		if price <= st.entry*(1-h.slPriceFrac) {
			pnl := st.qty*(price-st.entry) - h.feeRT*st.notional
			h.closeTrade(st, sym, "SL", price, pnl, now)
			return
		}
		st.lastPrice, st.haveLast = price, true
		if haveQ && quoteVol >= 0 {
			st.lastQuoteVol, st.haveQuoteVol = quoteVol, true
		}
		return
	}

	hL := h.histLen
	if cap(st.retBuf) < hL {
		st.retBuf = make([]float64, hL)
		st.dqBuf = make([]float64, hL)
	}
	st.retBuf = st.retBuf[:hL]
	st.dqBuf = st.dqBuf[:hL]

	if !st.haveLast {
		st.lastPrice, st.haveLast = price, true
		if haveQ && quoteVol >= 0 {
			st.lastQuoteVol, st.haveQuoteVol = quoteVol, true
		}
		st.pumpArmCnt = 0
		return
	}

	deltaPct := (price - st.lastPrice) / st.lastPrice * 100
	st.lastPrice = price

	var dQ float64
	var haveDQ bool
	if haveQ && quoteVol >= 0 {
		if st.haveQuoteVol && st.lastQuoteVol > 0 && quoteVol < st.lastQuoteVol*0.5 {
			st.lastQuoteVol = quoteVol
			st.pumpArmCnt = 0
			idx := st.barsSeen % hL
			st.retBuf[idx] = deltaPct
			st.dqBuf[idx] = 0
			st.barsSeen++
			return
		}
		if st.haveQuoteVol {
			dQ = quoteVol - st.lastQuoteVol
			haveDQ = dQ >= 0
		}
		st.lastQuoteVol, st.haveQuoteVol = quoteVol, true
	}

	idx := st.barsSeen % hL
	st.retBuf[idx] = deltaPct
	if haveDQ && dQ > 0 {
		st.dqBuf[idx] = dQ
	} else {
		st.dqBuf[idx] = 0
	}
	st.barsSeen++

	ready := st.barsSeen >= h.warmupBars && st.barsSeen >= h.histLen
	if !ready {
		if deltaPct <= 0 {
			st.pumpArmCnt = 0
		}
		return
	}

	if h.cooldown > 0 && !st.lastSignalAt.IsZero() && now.Sub(st.lastSignalAt) < h.cooldown {
		if deltaPct <= 0 {
			st.pumpArmCnt = 0
		}
		return
	}

	medAbs := st.medianAbsRet(st.scratch, hL)
	medDq := st.medianDQ(st.scratch, hL)
	priceTh := h.pumpPct
	if h.pumpFloorPct > priceTh {
		priceTh = h.pumpFloorPct
	}
	if x := h.retMult * medAbs; x > priceTh {
		priceTh = x
	}
	volTh := h.minQuoteDlt
	if x := h.volMult * medDq; x > volTh {
		volTh = x
	}

	real := deltaPct > 0 && deltaPct <= h.maxPumpPct &&
		deltaPct >= priceTh && haveDQ && dQ >= volTh

	if real {
		st.pumpArmCnt++
	} else if deltaPct <= 0 {
		st.pumpArmCnt = 0
	}

	if st.pumpArmCnt >= h.pumpTicks {
		st.pumpArmCnt = 0
		st.lastSignalAt = now
		st.openSim(price, now)
	}
}

func newSymState(notional float64, histLen int) *symState {
	return &symState{
		notional: notional,
		scratch:  make([]float64, 0, histLen),
		retBuf:   make([]float64, histLen),
		dqBuf:    make([]float64, histLen),
	}
}

func (st *symState) openSim(price float64, now time.Time) {
	st.inPos = true
	st.entry = price
	st.qty = st.notional / price
	st.openedAt = now
}

func (h *hub) closeTrade(st *symState, sym, reason string, exitPrice float64, pnl float64, now time.Time) {
	hold := now.Sub(st.openedAt)
	roi := pnl / h.marginUSDT * 100
	h.addPnL(pnl)
	h.closedTrades.Add(1)
	log.Printf("[%s] %s entry=%.8f exit=%.8f pnl=%.6f USDT (%.2f%% на $1) hold=%s",
		sym, reason, st.entry, exitPrice, pnl, roi, hold.Truncate(time.Second))
	st.inPos = false
	st.entry = 0
	st.qty = 0
	st.pumpArmCnt = 0
}

func chunkStrings(in []string, n int) [][]string {
	var out [][]string
	for i := 0; i < len(in); i += n {
		j := i + n
		if j > len(in) {
			j = len(in)
		}
		out = append(out, in[i:j])
	}
	return out
}

func streamsURL(baseWS string, syms []string) string {
	var b strings.Builder
	b.WriteString(strings.TrimSuffix(baseWS, "/"))
	b.WriteString("/stream?streams=")
	for i, s := range syms {
		if i > 0 {
			b.WriteByte('/')
		}
		b.WriteString(strings.ToLower(s) + "@miniTicker")
	}
	return b.String()
}

func runConnection(ctx context.Context, url string, h *hub, wg *sync.WaitGroup) {
	defer wg.Done()
	d := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
	}
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		conn, _, err := d.Dial(url, nil)
		if err != nil {
			log.Printf("ws dial: %v, retry %v", err, backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		conn.SetReadDeadline(time.Now().Add(readDeadLine))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(readDeadLine))
			return nil
		})
		go func() {
			t := time.NewTicker(20 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					_ = conn.WriteMessage(websocket.PingMessage, nil)
				}
			}
		}()

		readLoop := true
		for readLoop {
			select {
			case <-ctx.Done():
				_ = conn.Close()
				return
			default:
			}
			_, msg, err := conn.ReadMessage()
			if err != nil {
				log.Printf("ws read: %v", err)
				_ = conn.Close()
				readLoop = false
				break
			}
			conn.SetReadDeadline(time.Now().Add(readDeadLine))
			now := time.Now()
			sym, px, qv, ok := parseMiniPayload(msg)
			if !ok {
				continue
			}
			haveQ := qv >= 0
			h.processTick(sym, px, qv, haveQ, now)
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		log.Printf("переподключение ws...")
		time.Sleep(500 * time.Millisecond)
	}
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	em := loadEnv(".env")
	baseREST := strings.TrimSuffix(envGet(em, "BINANCE_FAPI_URL", "https://fapi.binance.com"), "/")
	baseWS := strings.TrimSuffix(envGet(em, "BINANCE_FSTREAM_WS", "wss://fstream.binancefuture.com"), "/")

	pumpPct := envGetFloat(em, "PUMP_PCT_1S", defaultPumpMinPct)
	if pumpPct <= 0 {
		pumpPct = defaultPumpMinPct
	}
	pumpFloor := envGetFloat(em, "PUMP_FLOOR_PCT", defaultPumpFloorPct)
	retMult := envGetFloat(em, "PUMP_RET_MULT", defaultRetMult)
	if retMult < 2 {
		retMult = 2
	}
	minQD := envGetFloat(em, "PUMP_MIN_QUOTE_DELTA", defaultMinQuoteDelta)
	volMult := envGetFloat(em, "PUMP_VOL_MULT", defaultVolMult)
	if volMult < 1.5 {
		volMult = 1.5
	}
	histLen := envGetInt(em, "PUMP_HIST_LEN", defaultHistLen)
	if histLen < 20 {
		histLen = 20
	}
	warmup := envGetInt(em, "PUMP_WARMUP_BARS", defaultWarmupBars)
	if warmup < histLen {
		warmup = histLen
	}
	maxPump := envGetFloat(em, "PUMP_MAX_TICK_PCT", defaultMaxPumpPct)
	if maxPump <= 0 {
		maxPump = defaultMaxPumpPct
	}
	cdSec := envGetInt(em, "PUMP_COOLDOWN_SEC", defaultCooldownSec)
	var cd time.Duration
	if cdSec > 0 {
		cd = time.Duration(cdSec) * time.Second
	}
	symsMin := envGetInt(em, "MIN_SYMBOLS", minSymbols)
	if symsMin < minSymbols {
		symsMin = minSymbols
	}
	m := envGetFloat(em, "MARGIN_USDT", marginUSDT)
	lev := envGetFloat(em, "LEVERAGE", leverage)
	tpm := envGetFloat(em, "TP_MARGIN_PCT", tpMarginPct)
	slm := envGetFloat(em, "SL_MARGIN_PCT", slMarginPct)
	fee := envGetFloat(em, "FEE_ROUND_TRIP", feeRoundTrip)

	tpMove := priceMoveForMarginPct(tpm, lev)
	slMove := priceMoveForMarginPct(slm, lev)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	symbols, err := fetchUniverse(ctx, baseREST, symsMin)
	if err != nil {
		log.Fatalf("universe: %v", err)
	}
	log.Printf("REST: %s — символов USDT PERPETUAL (топ по объёму, до %d): %d", baseREST, symsMin, len(symbols))

	pticks := envGetInt(em, "PUMP_CONFIRM_TICKS", 2)
	if pticks < 1 {
		pticks = 1
	}
	h := &hub{
		pumpPct:      pumpPct,
		pumpFloorPct: pumpFloor,
		pumpTicks:    pticks,
		retMult:      retMult,
		minQuoteDlt:  minQD,
		volMult:      volMult,
		warmupBars:   warmup,
		maxPumpPct:   maxPump,
		cooldown:     cd,
		histLen:      histLen,
		tpPriceFrac:  1 + tpMove,
		slPriceFrac:  slMove,
		feeRT:        fee,
		marginUSDT:   m,
		notionalUSDT: m * lev,
	}
	for _, s := range symbols {
		h.states.Store(s, &symState{notional: m * lev})
	}

	batches := chunkStrings(symbols, streamsPerConn)
	var wg sync.WaitGroup
	for _, b := range batches {
		u := streamsURL(baseWS, b)
		wg.Add(1)
		go runConnection(ctx, u, h, &wg)
	}

	log.Printf("WS: %s — conn=%d streams/sock≤%d | «реальный» памп: цена ≥ max(%.2f%%, floor %.2f%%, %.1f×med|1s|), Δq ≥ max(%.0fk, %.1f×medΔq), окно=%d разгон=%d тик max=%.1f%% подряд=%d cooldown=%s | симуляция $%.2f ×%.0f TP=%.0f%% SL=%.0f%% (цена +%.4f%% / −%.4f%%)",
		baseWS, len(batches), streamsPerConn, pumpPct, pumpFloor, retMult, minQD/1000, volMult, histLen, warmup, maxPump, pticks, cd, m, lev, tpm, slm, tpMove*100, slMove*100)

	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				pnl := float64FromBits(h.totalPnL.Load())
				log.Printf("STATS: сделок=%d суммарный PnL=%.4f USDT (симуляция)",
					h.closedTrades.Load(), pnl)
			}
		}
	}()

	<-ctx.Done()
	log.Printf("остановка...")
	wg.Wait()
	pnl := float64FromBits(h.totalPnL.Load())
	log.Printf("Итого: закрыто сделок=%d PnL=%.6f USDT", h.closedTrades.Load(), pnl)
}
