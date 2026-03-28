// USDT-M фьючерсы: вход по раннему сигналу aggTrade (микро-окно) и/или мини-тикеру + Δq.
// Выход по TP/SL/таймауту; симуляция маржа×плечо.
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
	minSymbols        = 400
	defaultStreamsConn = 120
	defaultWSDeadlineS = 60
	defaultStatsSec    = 10

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

func envGetDurationSec(em envMap, key string, defSec int) time.Duration {
	n := envGetInt(em, key, defSec)
	if n <= 0 {
		n = defSec
	}
	return time.Duration(n) * time.Second
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

// Binance в WS отдаёт c/q как строку или число — оба варианта должны разбираться.
func parseFloatish(v any) (float64, bool) {
	if v == nil {
		return 0, false
	}
	switch x := v.(type) {
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, false
		}
		return x, true
	case string:
		x = strings.TrimSpace(x)
		if x == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(x, 64)
		if err != nil || math.IsNaN(f) {
			return 0, false
		}
		return f, true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil && !math.IsNaN(f)
	default:
		return 0, false
	}
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

func fetchUniverse(ctx context.Context, baseREST string, needMin int, minLastPrice float64) ([]string, error) {
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
		return cands, nil // fallback: без сортировки и без фильтра по цене
	}
	vol := make(map[string]float64, len(tickers))
	lastPx := make(map[string]float64, len(tickers))
	for _, t := range tickers {
		v, _ := strconv.ParseFloat(t.QuoteVolume, 64)
		vol[t.Symbol] = v
		if lp, err := strconv.ParseFloat(t.LastPrice, 64); err == nil && lp > 0 {
			lastPx[t.Symbol] = lp
		}
	}
	var pool []string
	if minLastPrice <= 0 {
		pool = cands
	} else {
		for _, s := range cands {
			lp, ok := lastPx[s]
			if ok && lp >= minLastPrice {
				pool = append(pool, s)
			}
		}
		if len(pool) == 0 {
			return nil, fmt.Errorf("после MIN_LAST_PRICE_USDT=%.6f не осталось символов", minLastPrice)
		}
	}
	sort.Slice(pool, func(i, j int) bool {
		return vol[pool[i]] > vol[pool[j]]
	})
	if len(pool) > needMin {
		pool = pool[:needMin]
	}
	return pool, nil
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
	opening      bool // в процессе выставления лайв-ордера
	closing      bool // в процессе закрытия лайв-ордера
	posLev       int  // фактическое плечо лайв-позиции (0 = симуляция / не задано)

	retBuf   []float64 // кольцо: % изменения цены за тик (~1 с)
	dqBuf    []float64 // кольцо: Δq USDT за тик; 0 = нет данных за эту секунду
	scratch  []float64
	barsSeen int // обработано тиков цены (вне позиции и в момент смены last)

	// aggTrade: скользящее окно последних сделок (по времени T с биржи)
	aggBuf []aggPt
}

type aggPt struct {
	ts         int64
	price      float64
	quoteUSDT  float64
	buyerMaker bool
}

func priceMoveForMarginPct(marginPct float64, lev float64) float64 {
	return (marginPct / 100) / lev
}

// ---- WS ----

type miniWrap struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

func parseMiniPayload(raw []byte) (sym string, price float64, quoteVol float64, ok bool) {
	var w miniWrap
	if json.Unmarshal(raw, &w) == nil && len(w.Data) > 0 {
		raw = w.Data
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", 0, 0, false
	}
	sym, _ = m["s"].(string)
	if sym == "" {
		return "", 0, 0, false
	}
	p, okp := parseFloatish(m["c"])
	if !okp || p <= 0 {
		return "", 0, 0, false
	}
	qv := -1.0
	if q, okq := parseFloatish(m["q"]); okq && q >= 0 {
		qv = q
	}
	return sym, p, qv, true
}

// aggTrade: p, q, T (ms), m = isBuyerMaker (true → пассивный buyer, агрессивный sell)
func parseAggPayload(raw []byte) (sym string, price, qty float64, buyerMaker bool, tradeMs int64, ok bool) {
	var w miniWrap
	if json.Unmarshal(raw, &w) == nil && len(w.Data) > 0 {
		raw = w.Data
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", 0, 0, false, 0, false
	}
	if ev, ok := m["e"].(string); ok && ev != "" && ev != "aggTrade" {
		return "", 0, 0, false, 0, false
	}
	sym, _ = m["s"].(string)
	p, okp := parseFloatish(m["p"])
	q, okq := parseFloatish(m["q"])
	if !okp || !okq || sym == "" || p <= 0 || q <= 0 {
		return "", 0, 0, false, 0, false
	}
	Tf, okT := parseFloatish(m["T"])
	if !okT {
		return "", 0, 0, false, 0, false
	}
	tradeMs = int64(Tf)
	switch x := m["m"].(type) {
	case bool:
		buyerMaker = x
	case string:
		buyerMaker = strings.EqualFold(x, "true")
	default:
		buyerMaker = false
	}
	return sym, p, q, buyerMaker, tradeMs, true
}

func streamsKindURL(baseWS string, syms []string, kind string) string {
	var b strings.Builder
	b.WriteString(strings.TrimSuffix(baseWS, "/"))
	b.WriteString("/stream?streams=")
	for i, s := range syms {
		if i > 0 {
			b.WriteByte('/')
		}
		b.WriteString(strings.ToLower(s) + "@" + kind)
	}
	return b.String()
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

	tpMarginPct float64 // из .env — для TP/SL по цене с учётом плеча
	slMarginPct float64
	feeRT       float64
	marginUSDT   float64
	notionalUSDT float64
	totalPnL     atomic.Uint64 // bits of float64
	closedTrades atomic.Uint64

	dbg      bool
	dbgWS    atomic.Uint64 // разобранных WS-сообщений мини-тикера
	dbgWithQ atomic.Uint64 // из них с валидным q
	dbgAgg   atomic.Uint64

	miniEntry bool // открывать ли позицию по мини-тикеру (если false — только agg)

	aggEnabled    bool
	aggWindowMs   int64
	aggMinQuote   float64
	aggMinMovePct float64
	aggTakerBuy   bool // только агрессивные покупки (!buyerMaker)

	maxHold time.Duration

	// Реальная торговля (REST); выход по цене с мини-тикера → MARKET reduceOnly.
	fapi       *fapiClient
	live       bool
	liveHedge  bool // true = аккаунт в hedge mode (positionSide LONG)
	liveMaxPos int
	levInt     int
	restMu     sync.Mutex
	openCnt    atomic.Int32
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
		if h.live && st.closing {
			return
		}
		levM := float64(h.levInt)
		if st.posLev > 0 {
			levM = float64(st.posLev)
		}
		tpMove := priceMoveForMarginPct(h.tpMarginPct, levM)
		slMove := priceMoveForMarginPct(h.slMarginPct, levM)
		tpFrac := 1 + tpMove
		if h.maxHold > 0 && now.Sub(st.openedAt) >= h.maxHold {
			if h.live {
				if st.closing {
					return
				}
				st.closing = true
				go h.doCloseAsync(sym, st, "TIME", price, now)
				return
			}
			pnl := st.qty*(price-st.entry) - h.feeRT*st.notional
			h.closeTrade(st, sym, "TIME", price, pnl, now)
			return
		}
		if price >= st.entry*tpFrac {
			if h.live {
				if st.closing {
					return
				}
				st.closing = true
				go h.doCloseAsync(sym, st, "TP", price, now)
				return
			}
			pnl := st.qty*(price-st.entry) - h.feeRT*st.notional
			h.closeTrade(st, sym, "TP", price, pnl, now)
			return
		}
		if price <= st.entry*(1-slMove) {
			if h.live {
				if st.closing {
					return
				}
				st.closing = true
				go h.doCloseAsync(sym, st, "SL", price, now)
				return
			}
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

	if !h.miniEntry {
		st.lastPrice = price
		st.haveLast = true
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
	if h.retMult > 0 {
		if x := h.retMult * medAbs; x > priceTh {
			priceTh = x
		}
	}
	volTh := h.minQuoteDlt
	if h.volMult > 0 && medDq > 0 {
		if x := h.volMult * medDq; x > volTh {
			volTh = x
		}
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
		if h.live {
			if st.opening || st.inPos {
				return
			}
			st.opening = true
			go h.doOpenAsync(sym, st, price, now)
			return
		}
		st.openSim(price, now)
	}
}

func (h *hub) processAgg(sym string, price, qty float64, buyerMaker bool, tradeMs int64, now time.Time) {
	if !h.aggEnabled {
		return
	}
	v, _ := h.states.LoadOrStore(sym, newSymState(h.notionalUSDT, h.histLen))
	st := v.(*symState)

	st.mu.Lock()
	defer st.mu.Unlock()

	if st.inPos {
		return
	}
	if h.live && (st.opening || st.closing) {
		return
	}
	if h.cooldown > 0 && !st.lastSignalAt.IsZero() && now.Sub(st.lastSignalAt) < h.cooldown {
		return
	}

	quote := price * qty
	st.aggBuf = append(st.aggBuf, aggPt{ts: tradeMs, price: price, quoteUSDT: quote, buyerMaker: buyerMaker})
	if len(st.aggBuf) > 800 {
		st.aggBuf = st.aggBuf[len(st.aggBuf)-600:]
	}

	cutoff := tradeMs - h.aggWindowMs
	i := 0
	for i < len(st.aggBuf) && st.aggBuf[i].ts < cutoff {
		i++
	}
	if i > 0 {
		st.aggBuf = st.aggBuf[i:]
	}

	var minP, lastQual, sumQ float64
	var have bool
	for j := range st.aggBuf {
		pt := &st.aggBuf[j]
		if h.aggTakerBuy && pt.buyerMaker {
			continue
		}
		sumQ += pt.quoteUSDT
		if !have || pt.price < minP {
			minP = pt.price
		}
		lastQual = pt.price
		have = true
	}
	if !have || minP <= 0 {
		return
	}
	movePct := (lastQual - minP) / minP * 100
	if sumQ < h.aggMinQuote || movePct < h.aggMinMovePct || lastQual <= minP {
		return
	}
	st.lastSignalAt = now
	if h.live {
		if st.opening || st.inPos {
			return
		}
		st.opening = true
		go h.doOpenAsync(sym, st, lastQual, now)
		return
	}
	st.openSim(lastQual, now)
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
	mode := "симуляция"
	if h.live {
		mode = "лайв"
	}
	log.Printf("[%s] %s entry=%.8f exit=%.8f pnl=%.6f USDT (%.2f%% на $1) hold=%s [%s]",
		sym, reason, st.entry, exitPrice, pnl, roi, hold.Truncate(time.Second), mode)
	st.inPos = false
	st.entry = 0
	st.qty = 0
	st.posLev = 0
	st.pumpArmCnt = 0
}

func (h *hub) doOpenAsync(sym string, st *symState, refPrice float64, now time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	h.restMu.Lock()
	if int(h.openCnt.Load()) >= h.liveMaxPos {
		h.restMu.Unlock()
		st.mu.Lock()
		st.opening = false
		st.mu.Unlock()
		return
	}
	useLev, err := h.fapi.ensureLeverage(ctx, sym, h.levInt, h.notionalUSDT)
	if err != nil {
		h.restMu.Unlock()
		st.mu.Lock()
		st.opening = false
		st.mu.Unlock()
		log.Printf("[%s] leverage: %v", sym, err)
		return
	}
	if useLev != h.levInt {
		log.Printf("[%s] плечо по bracket: %d× (запрошено %d×), нотиoнал %.2f USDT",
			sym, useLev, h.levInt, h.marginUSDT*float64(useLev))
	}
	effNotional := h.marginUSDT * float64(useLev)
	qtyStr, _, err := h.fapi.PrepareQtyBuy(sym, effNotional, refPrice)
	if err != nil {
		h.restMu.Unlock()
		st.mu.Lock()
		st.opening = false
		st.mu.Unlock()
		log.Printf("[%s] подготовка qty: %v", sym, err)
		return
	}
	avg, q, err := h.fapi.MarketBuyLong(ctx, sym, qtyStr, h.liveHedge)
	h.restMu.Unlock()

	st.mu.Lock()
	defer st.mu.Unlock()
	st.opening = false
	if err != nil {
		log.Printf("[%s] market BUY: %v", sym, err)
		return
	}
	st.inPos = true
	if avg > 0 {
		st.entry = avg
	} else {
		st.entry = refPrice
	}
	st.qty = q
	st.openedAt = now
	st.posLev = useLev
	h.openCnt.Add(1)
	log.Printf("[%s] открыт лонг qty=%.8f avg=%.8f плечо=%d×", sym, q, st.entry, useLev)
}

func (h *hub) doCloseAsync(sym string, st *symState, reason string, markPrice float64, now time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	st.mu.Lock()
	entry := st.entry
	openQty := st.qty
	st.mu.Unlock()

	h.restMu.Lock()
	qtyStr, _, err := h.fapi.PrepareQtyClose(sym, openQty)
	if err != nil {
		h.restMu.Unlock()
		st.mu.Lock()
		st.closing = false
		st.mu.Unlock()
		log.Printf("[%s] закрытие qty: %v", sym, err)
		return
	}
	avg, qdone, err := h.fapi.MarketSellClose(ctx, sym, qtyStr, h.liveHedge)
	h.restMu.Unlock()

	st.mu.Lock()
	defer st.mu.Unlock()
	st.closing = false
	if err != nil {
		log.Printf("[%s] market SELL reduceOnly: %v", sym, err)
		return
	}
	pnl := qdone*(avg-entry) - h.feeRT*st.notional
	h.closeTrade(st, sym, reason, avg, pnl, now)
	h.openCnt.Add(-1)
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
	return streamsKindURL(baseWS, syms, "miniTicker")
}

func runConnection(ctx context.Context, url string, h *hub, readDeadline time.Duration, wg *sync.WaitGroup) {
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
		conn.SetReadDeadline(time.Now().Add(readDeadline))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(readDeadline))
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
			conn.SetReadDeadline(time.Now().Add(readDeadline))
			now := time.Now()
			sym, px, qv, ok := parseMiniPayload(msg)
			if !ok {
				continue
			}
			haveQ := qv >= 0
			if h.dbg {
				h.dbgWS.Add(1)
				if haveQ {
					h.dbgWithQ.Add(1)
				}
			}
			h.processTick(sym, px, qv, haveQ, now)
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		log.Printf("переподключение ws (mini)...")
		time.Sleep(500 * time.Millisecond)
	}
}

func runAggConnection(ctx context.Context, url string, h *hub, readDeadline time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()
	d := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		conn, _, err := d.Dial(url, nil)
		if err != nil {
			log.Printf("ws agg dial: %v, retry %v", err, backoff)
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
		conn.SetReadDeadline(time.Now().Add(readDeadline))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(readDeadline))
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
				log.Printf("ws agg read: %v", err)
				_ = conn.Close()
				readLoop = false
				break
			}
			conn.SetReadDeadline(time.Now().Add(readDeadline))
			now := time.Now()
			sym, px, qty, bm, tms, ok := parseAggPayload(msg)
			if !ok {
				continue
			}
			if h.dbg {
				h.dbgAgg.Add(1)
			}
			h.processAgg(sym, px, qty, bm, tms, now)
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		log.Printf("переподключение ws (agg)...")
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
	// 0 = не умножать на med|1s| (иначе на BTC/ETH порог цены уезжает вверх)
	retMult := envGetFloat(em, "PUMP_RET_MULT", defaultRetMult)
	if retMult < 0 {
		retMult = defaultRetMult
	} else if retMult > 0 && retMult < 2 {
		retMult = 2
	}
	minQD := envGetFloat(em, "PUMP_MIN_QUOTE_DELTA", defaultMinQuoteDelta)
	// 0 = только абсолютный пол PUMP_MIN_QUOTE_DELTA (иначе volTh = N×medΔq недостижим на ликвидных парах)
	volMult := envGetFloat(em, "PUMP_VOL_MULT", defaultVolMult)
	if volMult < 0 {
		volMult = defaultVolMult
	} else if volMult > 0 && volMult < 1.5 {
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
	if symsMin < 1 {
		symsMin = minSymbols
	}
	minLastPx := envGetFloat(em, "MIN_LAST_PRICE_USDT", 0)
	if minLastPx < 0 {
		minLastPx = 0
	}
	streamsConn := envGetInt(em, "STREAMS_PER_CONN", defaultStreamsConn)
	if streamsConn < 30 {
		streamsConn = 30
	}
	if streamsConn > 300 {
		streamsConn = 300
	}
	readDeadline := envGetDurationSec(em, "WS_READ_DEADLINE_SEC", defaultWSDeadlineS)
	statsEvery := envGetDurationSec(em, "STATS_INTERVAL_SEC", defaultStatsSec)
	m := envGetFloat(em, "MARGIN_USDT", marginUSDT)
	lev := envGetFloat(em, "LEVERAGE", leverage)
	tpm := envGetFloat(em, "TP_MARGIN_PCT", tpMarginPct)
	slm := envGetFloat(em, "SL_MARGIN_PCT", slMarginPct)
	fee := envGetFloat(em, "FEE_ROUND_TRIP", feeRoundTrip)

	tpMove := priceMoveForMarginPct(tpm, lev)
	slMove := priceMoveForMarginPct(slm, lev)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	symbols, err := fetchUniverse(ctx, baseREST, symsMin, minLastPx)
	if err != nil {
		log.Fatalf("universe: %v", err)
	}
	if minLastPx > 0 {
		log.Printf("REST: %s — символов USDT PERPETUAL (топ по объёму, last≥%.6f, до %d): %d",
			baseREST, minLastPx, symsMin, len(symbols))
	} else {
		log.Printf("REST: %s — символов USDT PERPETUAL (топ по объёму, до %d): %d", baseREST, symsMin, len(symbols))
	}

	pticks := envGetInt(em, "PUMP_CONFIRM_TICKS", 2)
	if pticks < 1 {
		pticks = 1
	}
	dbg := strings.TrimSpace(envGet(em, "PUMP_DEBUG", "")) == "1"

	aggEnabled := strings.TrimSpace(envGet(em, "AGG_ENABLED", "")) == "1"
	aggWinMs := int64(envGetInt(em, "AGG_WINDOW_MS", 350))
	if aggWinMs < 80 {
		aggWinMs = 80
	}
	if aggWinMs > 5000 {
		aggWinMs = 5000
	}
	aggMinQ := envGetFloat(em, "AGG_MIN_QUOTE_USDT", 30_000)
	if aggMinQ < 0 {
		aggMinQ = 30_000
	}
	aggMinMv := envGetFloat(em, "AGG_MIN_MOVE_PCT", 0.06)
	if aggMinMv < 0 {
		aggMinMv = 0.06
	}
	aggTaker := strings.TrimSpace(envGet(em, "AGG_TAKER_BUY_ONLY", "1")) != "0"

	miniEntS := strings.TrimSpace(envGet(em, "PUMP_MINI_ENTRY", ""))
	var miniEntry bool
	switch miniEntS {
	case "1":
		miniEntry = true
	case "0":
		miniEntry = false
	default:
		miniEntry = !aggEnabled
	}
	if !aggEnabled && !miniEntry {
		miniEntry = true
	}

	maxHoldSec := envGetInt(em, "MAX_HOLD_SEC", 0)
	var maxHold time.Duration
	if maxHoldSec > 0 {
		maxHold = time.Duration(maxHoldSec) * time.Second
	}

	liveTrading := strings.TrimSpace(envGet(em, "LIVE_TRADING", "")) == "1"
	liveMaxPos := envGetInt(em, "LIVE_MAX_POSITIONS", 1)
	if liveMaxPos < 1 {
		liveMaxPos = 1
	}
	liveHedge := strings.TrimSpace(envGet(em, "LIVE_HEDGE_MODE", "")) == "1"

	levInt := int(lev + 0.5)
	if levInt < 1 {
		levInt = 1
	}
	if levInt > 125 {
		levInt = 125
	}

	h := &hub{
		pumpPct:       pumpPct,
		pumpFloorPct:  pumpFloor,
		pumpTicks:     pticks,
		retMult:       retMult,
		minQuoteDlt:   minQD,
		volMult:       volMult,
		warmupBars:    warmup,
		maxPumpPct:    maxPump,
		cooldown:      cd,
		histLen:       histLen,
		tpMarginPct:   tpm,
		slMarginPct:   slm,
		feeRT:         fee,
		marginUSDT:    m,
		notionalUSDT:  m * lev,
		dbg:           dbg,
		miniEntry:     miniEntry,
		aggEnabled:    aggEnabled,
		aggWindowMs:   aggWinMs,
		aggMinQuote:   aggMinQ,
		aggMinMovePct: aggMinMv,
		aggTakerBuy:   aggTaker,
		maxHold:       maxHold,
		live:          false,
		liveHedge:     liveHedge,
		liveMaxPos:    liveMaxPos,
		levInt:        levInt,
	}

	if liveTrading {
		key := strings.TrimSpace(envGet(em, "BINANCE_API_KEY", ""))
		sec := strings.TrimSpace(envGet(em, "BINANCE_API_SECRET", ""))
		if key == "" || sec == "" {
			log.Fatal("LIVE_TRADING=1: задайте BINANCE_API_KEY и BINANCE_API_SECRET (тот же тип счёта, что и BINANCE_FAPI_URL)")
		}
		fc := newFAPIClient(baseREST, key, sec)
		if err := fc.loadLotSpecs(ctx); err != nil {
			log.Fatalf("exchangeInfo (лайв): %v", err)
		}
		h.fapi = fc
		h.live = true
		log.Printf("РЕЖИМ ЛАЙВ: реальные MARKET-ордера на %s | max одновременных позиций=%d | hedge=%v | плечо=%d×",
			baseREST, liveMaxPos, liveHedge, levInt)
	}

	for _, s := range symbols {
		h.states.Store(s, &symState{notional: m * lev})
	}

	batches := chunkStrings(symbols, streamsConn)
	var wg sync.WaitGroup
	for _, b := range batches {
		u := streamsURL(baseWS, b)
		wg.Add(1)
		go runConnection(ctx, u, h, readDeadline, &wg)
	}
	if aggEnabled {
		for _, b := range batches {
			u := streamsKindURL(baseWS, b, "aggTrade")
			wg.Add(1)
			go runAggConnection(ctx, u, h, readDeadline, &wg)
		}
	}

	priceRule := fmt.Sprintf("max(%.2f%%, floor %.2f%%, %.1f×med|1s|)", pumpPct, pumpFloor, retMult)
	if retMult <= 0 {
		priceRule = fmt.Sprintf("max(%.2f%%, floor %.2f%%) без med|1s|", pumpPct, pumpFloor)
	}
	volRule := fmt.Sprintf("max(%.0fk, %.1f×medΔq)", minQD/1000, volMult)
	if volMult <= 0 {
		volRule = fmt.Sprintf("≥%.0fk USDT за ~1с (без medΔq)", minQD/1000)
	}
	aggRule := "выкл"
	if aggEnabled {
		taker := "да"
		if !aggTaker {
			taker = "нет"
		}
		aggRule = fmt.Sprintf("окно %dms, ≥%.0fk USDT агр. объёма, рост min→last ≥%.3f%%, тейкер-лонг %s",
			aggWinMs, aggMinQ/1000, aggMinMv, taker)
	}
	miniRule := "да"
	if !miniEntry {
		miniRule = "нет"
	}
	aggConnN := 0
	if aggEnabled {
		aggConnN = len(batches)
	}
	maxHoldStr := "выкл"
	if maxHold > 0 {
		maxHoldStr = maxHold.String()
	}
	modeStr := "симуляция"
	if h.live {
		modeStr = fmt.Sprintf("ЛАЙВ (max поз=%d)", liveMaxPos)
	}
	log.Printf("WS: %s — mini conn=%d agg conn=%d | вход agg: %s | вход mini: %s | мини памп: цена ≥ %s; Δq %s; окно=%d разгон=%d max=%.1f%% подряд=%d cd=%s | max_hold=%s | %s $%.2f ×%.0f TP=%.0f%% SL=%.0f%% (цена +%.4f%% / −%.4f%%)",
		baseWS, len(batches), aggConnN, aggRule, miniRule, priceRule, volRule, histLen, warmup, maxPump, pticks, cd, maxHoldStr, modeStr, m, lev, tpm, slm, tpMove*100, slMove*100)

	tick := time.NewTicker(statsEvery)
	defer tick.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				pnl := float64FromBits(h.totalPnL.Load())
				if h.dbg {
					log.Printf("STATS: сделок=%d PnL=%.4f USDT | dbg: mini=%d с_q=%d agg=%d",
						h.closedTrades.Load(), pnl, h.dbgWS.Load(), h.dbgWithQ.Load(), h.dbgAgg.Load())
				} else {
					log.Printf("STATS: сделок=%d суммарный PnL=%.4f USDT (симуляция)",
						h.closedTrades.Load(), pnl)
				}
			}
		}
	}()

	<-ctx.Done()
	log.Printf("остановка...")
	wg.Wait()
	pnl := float64FromBits(h.totalPnL.Load())
	log.Printf("Итого: закрыто сделок=%d PnL=%.6f USDT", h.closedTrades.Load(), pnl)
}
