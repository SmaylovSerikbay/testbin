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
	marginUSDT         = 1.0
	leverage           = 20.0
	tpMarginPct        = 60.0   // тейк-профит в % от маржи
	slMarginPct        = 15.0   // стоп в % от маржи
	feeRoundTrip       = 0.0008 // ~0.04% вход + выход на нотиoнал (оценка)
	minSymbols         = 400
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
	Symbol      string `json:"symbol"`
	QuoteVolume string `json:"quoteVolume"`
	LastPrice   string `json:"lastPrice"`
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
	opening      bool    // в процессе выставления лайв-ордера
	closing      bool    // в процессе закрытия лайв-ордера
	posLev       int     // фактическое плечо лайв-позиции (0 = симуляция / не задано)
	peakPx       float64 // максимум цены в позиции (для трейлинга)
	trailArmed   bool    // был ли откат от пика после «настоящего» движения (с учётом комиссии)

	retBuf   []float64 // кольцо: % изменения цены за тик (~1 с)
	dqBuf    []float64 // кольцо: Δq USDT за тик; 0 = нет данных за эту секунду
	scratch  []float64
	barsSeen int // обработано тиков цены (вне позиции и в момент смены last)

	// aggTrade: скользящее окно последних сделок (по времени T с биржи)
	aggBuf []aggPt

	flashBuf []flashSample // только в позиции: недавние цены для FLASH-выхода
}

type aggPt struct {
	ts         int64
	price      float64
	quoteUSDT  float64
	buyerMaker bool
}

// flashSample — точки для детектора резкого слива за секунду-две (мини-тикер).
type flashSample struct {
	t time.Time
	p float64
}

func priceMoveForMarginPct(marginPct float64, lev float64) float64 {
	return (marginPct / 100) / lev
}

func (h *hub) pushFlashSample(st *symState, now time.Time, price float64) {
	if h.flashWindow <= 0 {
		return
	}
	st.flashBuf = append(st.flashBuf, flashSample{t: now, p: price})
	cutoff := now.Add(-h.flashWindow)
	i := 0
	for i < len(st.flashBuf) && st.flashBuf[i].t.Before(cutoff) {
		i++
	}
	if i > 0 {
		copy(st.flashBuf, st.flashBuf[i:])
		st.flashBuf = st.flashBuf[:len(st.flashBuf)-i]
	}
	if len(st.flashBuf) > 48 {
		st.flashBuf = st.flashBuf[len(st.flashBuf)-48:]
	}
}

// checkFlashDump — всплеск вниз внутри короткого окна (манипуляция / резкий разворот).
func (h *hub) checkFlashDump(st *symState, price float64, now time.Time) bool {
	if h.flashWindow <= 0 || len(st.flashBuf) < 2 {
		return false
	}
	var maxP float64
	for _, s := range st.flashBuf {
		if s.p > maxP {
			maxP = s.p
		}
	}
	if h.flashDropFrac > 0 && maxP > 0 && price <= maxP*(1-h.flashDropFrac) {
		return true
	}
	old := st.flashBuf[0]
	if now.Sub(old.t) < h.flashMinSpan || old.p <= 0 {
		return false
	}
	if h.flashSlopeFrac > 0 && (price-old.p)/old.p <= -h.flashSlopeFrac {
		return true
	}
	return false
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
	pumpPct      float64  // минимальный % движения (планка с env)
	pumpFloorPct float64  // доп. нижняя планка против шума
	pumpTicks    int
	retMult      float64
	minQuoteDlt  float64
	volMult      float64
	warmupBars   int
	maxPumpPct   float64
	cooldown     time.Duration
	histLen      int

	tpMarginPct  float64 // из .env — для TP/SL по цене с учётом плеча
	slMarginPct  float64
	feeRT        float64
	marginUSDT   float64
	notionalUSDT float64
	totalPnL     atomic.Uint64 // bits of float64
	closedTrades atomic.Uint64

	dbg      bool
	dbgWS    atomic.Uint64 // разобранных WS-сообщений мини-тикера
	dbgWithQ atomic.Uint64 // из них с валидным q
	dbgAgg   atomic.Uint64

	miniEntry bool // открывать ли позицию по мини-тикеру (если false — только agg)

	aggEnabled      bool
	aggWindowMs     int64
	aggMinQuote     float64
	aggMinMovePct   float64
	aggNearPeakFrac float64 // 0 = выкл; последняя цена ≥ max окна × (1−frac)
	aggTakerBuy     bool    // только агрессивные покупки (!buyerMaker)
	// Анти-фейк: рост от первой квалиф. сделки в окне (не только от min к last) + мин. число тейкер-баёв
	aggMinLastOverFirstPct float64 // 0 = выкл; last ≥ first×(1+pct/100)
	aggMinQualTrades       int     // 0 = выкл

	maxHold time.Duration

	// Реальная торговля (REST); выход по цене с мини-тикера → MARKET reduceOnly.
	fapi       *fapiClient
	live       bool
	liveHedge  bool // true = аккаунт в hedge mode (positionSide LONG)
	liveMaxPos int
	levInt     int
	restMu     sync.Mutex
	openCnt    atomic.Int32

	// лайв: доля доступного баланса под новую позицию (0.88 = оставить запас под IM/комиссии)
	liveMarginFrac float64

	// выход «под памп»: быстрый scratch, трейлинг от пика, комиссия в пороге трейла
	scratchDur      time.Duration // 0 = выкл
	scratchMinDur   time.Duration // не раньше N в позиции (антимикрошум)
	scratchPull     float64       // доля цены: ниже входа на столько — «не угадали памп»
	trailBack       float64       // откат от peakPx для фиксации
	trailFeeExtra   float64       // сверх оценки round-trip комиссии для включения трейла (доля)
	trailMinNetUSDT float64       // TRAIL только если оценка net PnL ≥ этого (не фиксировать «трейл» в минус)

	// резкий «слив» после накачки: окно мини-тикера (часто ~1 с), быстрее обычного SL
	flashWindow    time.Duration // 0 = выкл
	flashDropFrac  float64       // цена ниже max в окне на эту долю → FLASH
	flashSlopeFrac float64       // падение от самой старой точки окна за min span → FLASH
	flashMinSpan   time.Duration

	// страховки лайва (новые входы)
	entriesPaused      atomic.Bool
	maxSessionLossUSDT float64 // суммарный PnL сессии ≤ -X → стоп входов (0 = выкл)
	minAvailToTrade    float64 // не открывать если available ниже (0 = выкл)
	maxConsecLosses    int     // подряд убыточных сделок (0 = выкл)
	consecLoss         atomic.Int32
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

		if price > st.peakPx {
			st.peakPx = price
		}
		var feeFrac float64
		if st.qty > 0 && st.entry > 0 {
			feeFrac = (h.feeRT * st.notional) / (st.qty * st.entry)
		}
		trailOnFrac := feeFrac + h.trailFeeExtra
		if st.peakPx >= st.entry*(1+trailOnFrac) {
			st.trailArmed = true
		}

		h.pushFlashSample(st, now, price)
		if h.checkFlashDump(st, price, now) {
			h.triggerClose(st, sym, "FLASH", price, now)
			return
		}

		// TP/SL до TIME: таймер не должен «перебивать» жёсткий стоп; убыток в % на маржу = движение цены × плечо.
		if price >= st.entry*tpFrac {
			h.triggerClose(st, sym, "TP", price, now)
			return
		}
		if h.slMarginPct > 0 && price <= st.entry*(1-slMove) {
			h.triggerClose(st, sym, "SL", price, now)
			return
		}
		if h.scratchDur > 0 && now.Sub(st.openedAt) < h.scratchDur &&
			(h.scratchMinDur <= 0 || now.Sub(st.openedAt) >= h.scratchMinDur) &&
			!st.trailArmed && st.peakPx < st.entry*(1+trailOnFrac) && price <= st.entry*(1-h.scratchPull) {
			h.triggerClose(st, sym, "SCRATCH", price, now)
			return
		}
		if st.trailArmed && st.peakPx > 0 && price <= st.peakPx*(1-h.trailBack) {
			netEst := st.qty*(price-st.entry) - h.feeRT*st.notional
			if netEst >= h.trailMinNetUSDT {
				h.triggerClose(st, sym, "TRAIL", price, now)
				return
			}
		}
		if h.maxHold > 0 && now.Sub(st.openedAt) >= h.maxHold {
			h.triggerClose(st, sym, "TIME", price, now)
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
			if h.entriesPaused.Load() || st.opening || st.inPos {
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

	var minP, maxP, lastQual, firstQual, sumQ float64
	var have bool
	qualN := 0
	for j := range st.aggBuf {
		pt := &st.aggBuf[j]
		if h.aggTakerBuy && pt.buyerMaker {
			continue
		}
		qualN++
		sumQ += pt.quoteUSDT
		if !have {
			minP, maxP = pt.price, pt.price
			firstQual = pt.price
		} else {
			if pt.price < minP {
				minP = pt.price
			}
			if pt.price > maxP {
				maxP = pt.price
			}
		}
		lastQual = pt.price
		have = true
	}
	if !have || minP <= 0 || maxP <= 0 {
		return
	}
	movePct := (lastQual - minP) / minP * 100
	if sumQ < h.aggMinQuote || movePct < h.aggMinMovePct || lastQual <= minP {
		return
	}
	if h.aggNearPeakFrac > 0 && lastQual < maxP*(1-h.aggNearPeakFrac) {
		return
	}
	if h.aggMinQualTrades > 0 && qualN < h.aggMinQualTrades {
		return
	}
	if h.aggMinLastOverFirstPct > 0 && firstQual > 0 {
		lof := (lastQual - firstQual) / firstQual * 100
		if lof < h.aggMinLastOverFirstPct {
			return
		}
	}
	st.lastSignalAt = now
	if h.live {
		if h.entriesPaused.Load() || st.opening || st.inPos {
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
	st.peakPx = price
	st.trailArmed = false
	st.flashBuf = st.flashBuf[:0]
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
	st.peakPx = 0
	st.trailArmed = false
	st.flashBuf = st.flashBuf[:0]
	st.pumpArmCnt = 0
	if h.live {
		h.applyLiveGuards(pnl)
	}
}

func (h *hub) pauseLiveEntries(reason string) {
	if !h.entriesPaused.CompareAndSwap(false, true) {
		return
	}
	log.Printf("СТОП-ВХОДЫ: %s — новые сделки отключены (открытая позиция закрывается по правилам). Перезапуск бота сбрасывает стоп.", reason)
}

func (h *hub) applyLiveGuards(lastPnl float64) {
	if lastPnl < -1e-12 {
		n := h.consecLoss.Add(1)
		if h.maxConsecLosses > 0 && int(n) >= h.maxConsecLosses {
			h.pauseLiveEntries(fmt.Sprintf("серия убытков %d подряд", n))
		}
	} else {
		h.consecLoss.Store(0)
	}
	tot := float64FromBits(h.totalPnL.Load())
	if h.maxSessionLossUSDT > 0 && tot <= -h.maxSessionLossUSDT {
		h.pauseLiveEntries(fmt.Sprintf("суммарный PnL сессии %.4f USDT (лимит −%.4f)", tot, h.maxSessionLossUSDT))
	}
}

func (h *hub) triggerClose(st *symState, sym, reason string, price float64, now time.Time) {
	if h.live {
		if st.closing {
			return
		}
		st.closing = true
		go h.doCloseAsync(sym, st, reason, price, now)
		return
	}
	pnl := st.qty*(price-st.entry) - h.feeRT*st.notional
	h.closeTrade(st, sym, reason, price, pnl, now)
}

func (h *hub) doOpenAsync(sym string, st *symState, refPrice float64, now time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	if h.entriesPaused.Load() {
		st.mu.Lock()
		st.opening = false
		st.mu.Unlock()
		return
	}

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
	wallet, avail, berr := h.fapi.FuturesUSDTBalance(ctx)
	if berr != nil {
		log.Printf("[%s] баланс фьючерсов: %v", sym, berr)
	}
	effMargin := h.marginUSDT
	if avail > 0 && h.liveMarginFrac > 0 && h.liveMarginFrac <= 1 {
		capM := avail * h.liveMarginFrac
		if capM < effMargin {
			effMargin = capM
		}
	}
	if h.minAvailToTrade > 0 && avail > 0 && avail < h.minAvailToTrade {
		h.restMu.Unlock()
		st.mu.Lock()
		st.opening = false
		st.mu.Unlock()
		log.Printf("[%s] пропуск: доступно %.4f < LIVE_MIN_AVAILABLE_USDT=%.4f", sym, avail, h.minAvailToTrade)
		return
	}
	if effMargin < 0.02 {
		h.restMu.Unlock()
		st.mu.Lock()
		st.opening = false
		st.mu.Unlock()
		log.Printf("[%s] маржа слишком мала (eff=%.4f USDT, доступно≈%.4f), пропуск", sym, effMargin, avail)
		return
	}
	effNotional := effMargin * float64(useLev)
	if wallet > 0 || avail > 0 {
		log.Printf("[%s] вход: кошелёк USDT-M=%.4f доступно=%.4f → маржа сделки≈%.4f нотиoнал≈%.2f",
			sym, wallet, avail, effMargin, effNotional)
	}
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
	st.notional = effNotional
	st.peakPx = st.entry
	st.trailArmed = false
	st.flashBuf = st.flashBuf[:0]
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
	liveTrading := strings.TrimSpace(envGet(em, "LIVE_TRADING", "")) == "1"
	var aggNearPeakFrac float64
	if v := strings.TrimSpace(envGet(em, "AGG_LAST_NEAR_PEAK_PCT", "")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			aggNearPeakFrac = f / 100
		}
	} else if liveTrading {
		aggNearPeakFrac = 0.001
	}
	aggMinLof := envGetFloat(em, "AGG_MIN_LAST_OVER_FIRST_PCT", 0)
	if aggMinLof < 0 {
		aggMinLof = 0
	}
	aggMinQualTr := envGetInt(em, "AGG_MIN_QUAL_TRADES", 0)
	if aggMinQualTr < 0 {
		aggMinQualTr = 0
	}
	if liveTrading {
		if mq := envGetFloat(em, "LIVE_AGG_QUOTE_MULT", 1.32); mq > 1 {
			aggMinQ *= mq
		}
		aggMinMv += envGetFloat(em, "LIVE_AGG_MOVE_ADD_PCT", 0.04)
		aggMinLof += envGetFloat(em, "LIVE_AGG_LAST_OVER_FIRST_ADD_PCT", 0)
		if add := envGetInt(em, "LIVE_AGG_MIN_QUAL_TRADES_ADD", 0); add > 0 {
			aggMinQualTr += add
		}
	}
	if aggMinLof < 0 {
		aggMinLof = 0
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

	scratchSec := envGetInt(em, "PUMP_SCRATCH_SEC", 22)
	var scratchDur time.Duration
	if scratchSec > 0 {
		scratchDur = time.Duration(scratchSec) * time.Second
	}
	scratchPull := envGetFloat(em, "PUMP_SCRATCH_PULL_PCT", 0.35) / 100
	if scratchPull < 0 {
		scratchPull = 0
	}
	if scratchPull == 0 && scratchDur > 0 {
		scratchPull = 0.003
	}
	scratchMinSec := envGetInt(em, "PUMP_SCRATCH_MIN_SEC", 3)
	var scratchMinDur time.Duration
	if scratchMinSec > 0 {
		scratchMinDur = time.Duration(scratchMinSec) * time.Second
	}
	if scratchDur > 0 && scratchMinDur > 0 && scratchMinDur >= scratchDur {
		scratchMinDur = 0
	}
	trailBack := envGetFloat(em, "PUMP_TRAIL_BACK_PCT", 0.45) / 100
	if trailBack <= 0 {
		trailBack = 0.0045
	}
	trailFeeExtra := envGetFloat(em, "PUMP_TRAIL_FEE_EXTRA_PCT", 0.06) / 100
	if trailFeeExtra < 0 {
		trailFeeExtra = 0
	}
	trailMinNet := envGetFloat(em, "PUMP_TRAIL_MIN_NET_USDT", 0.02)
	if trailMinNet < 0 {
		trailMinNet = 0
	}

	flashMs := envGetInt(em, "PUMP_FLASH_MS", 1200)
	if flashMs > 0 && flashMs < 250 {
		flashMs = 250
	}
	var flashWindow time.Duration
	if flashMs > 0 {
		flashWindow = time.Duration(flashMs) * time.Millisecond
	}
	flashDrop := envGetFloat(em, "PUMP_FLASH_DROP_PCT", 0.22) / 100
	if flashDrop < 0 {
		flashDrop = 0
	}
	flashSlope := envGetFloat(em, "PUMP_FLASH_SLOPE_PCT", 0.18) / 100
	if flashSlope < 0 {
		flashSlope = 0
	}
	flashMinMs := envGetInt(em, "PUMP_FLASH_MIN_MS", 350)
	if flashMinMs < 80 {
		flashMinMs = 80
	}
	flashMinSpan := time.Duration(flashMinMs) * time.Millisecond

	maxSessLoss := envGetFloat(em, "LIVE_MAX_SESSION_LOSS_USDT", 2.5)
	if maxSessLoss < 0 {
		maxSessLoss = 0
	}
	minAvailTrade := envGetFloat(em, "LIVE_MIN_AVAILABLE_USDT", 0.35)
	if minAvailTrade < 0 {
		minAvailTrade = 0
	}
	maxConsecLoss := envGetInt(em, "LIVE_MAX_CONSECUTIVE_LOSSES", 8)
	if maxConsecLoss < 0 {
		maxConsecLoss = 0
	}

	liveMaxPos := envGetInt(em, "LIVE_MAX_POSITIONS", 1)
	if liveMaxPos < 1 {
		liveMaxPos = 1
	}
	liveHedge := strings.TrimSpace(envGet(em, "LIVE_HEDGE_MODE", "")) == "1"

	bufPct := envGetFloat(em, "LIVE_MARGIN_BUFFER_PCT", 88)
	if bufPct <= 0 || bufPct > 100 {
		bufPct = 88
	}
	liveMarginFrac := bufPct / 100

	levInt := int(lev + 0.5)
	if levInt < 1 {
		levInt = 1
	}
	if levInt > 125 {
		levInt = 125
	}

	h := &hub{
		pumpPct:                pumpPct,
		pumpFloorPct:           pumpFloor,
		pumpTicks:              pticks,
		retMult:                retMult,
		minQuoteDlt:            minQD,
		volMult:                volMult,
		warmupBars:             warmup,
		maxPumpPct:             maxPump,
		cooldown:               cd,
		histLen:                histLen,
		tpMarginPct:            tpm,
		slMarginPct:            slm,
		feeRT:                  fee,
		marginUSDT:             m,
		notionalUSDT:           m * lev,
		dbg:                    dbg,
		miniEntry:              miniEntry,
		aggEnabled:             aggEnabled,
		aggWindowMs:            aggWinMs,
		aggMinQuote:            aggMinQ,
		aggMinMovePct:          aggMinMv,
		aggNearPeakFrac:        aggNearPeakFrac,
		aggTakerBuy:            aggTaker,
		aggMinLastOverFirstPct: aggMinLof,
		aggMinQualTrades:       aggMinQualTr,
		maxHold:                maxHold,
		live:                   false,
		liveHedge:              liveHedge,
		liveMaxPos:             liveMaxPos,
		levInt:                 levInt,
		liveMarginFrac:         liveMarginFrac,
		scratchDur:             scratchDur,
		scratchMinDur:          scratchMinDur,
		scratchPull:            scratchPull,
		trailBack:              trailBack,
		trailFeeExtra:          trailFeeExtra,
		trailMinNetUSDT:        trailMinNet,
		flashWindow:            flashWindow,
		flashDropFrac:          flashDrop,
		flashSlopeFrac:         flashSlope,
		flashMinSpan:           flashMinSpan,
		maxSessionLossUSDT:     maxSessLoss,
		minAvailToTrade:        minAvailTrade,
		maxConsecLosses:        maxConsecLoss,
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
		pw, pc := context.WithTimeout(ctx, 5*time.Second)
		if lat, err := fc.WarmupPing(pw); err != nil {
			log.Printf("FAPI прогрев ping: %v", err)
		} else {
			log.Printf("FAPI прогрев: ping за %v (HTTP/2 + keep-alive для ордеров)", lat.Round(time.Millisecond))
		}
		pc()
		h.fapi = fc
		h.live = true
		var guardParts []string
		if maxSessLoss > 0 {
			guardParts = append(guardParts, fmt.Sprintf("стоп при ΣPnL≤−%.2f USDT", maxSessLoss))
		}
		if minAvailTrade > 0 {
			guardParts = append(guardParts, fmt.Sprintf("вход только если avail≥%.2f", minAvailTrade))
		}
		if maxConsecLoss > 0 {
			guardParts = append(guardParts, fmt.Sprintf("стоп после %d убытков подряд", maxConsecLoss))
		}
		guardStr := "страховки нет (задайте LIVE_MAX_SESSION_LOSS_USDT и т.д.)"
		if len(guardParts) > 0 {
			guardStr = strings.Join(guardParts, "; ")
		}
		log.Printf("РЕЖИМ ЛАЙВ: %s | MARKET %s | max поз=%d | hedge=%v | %d× | буфер %.0f%%",
			guardStr, baseREST, liveMaxPos, liveHedge, levInt, bufPct)
		bctx, bcancel := context.WithTimeout(ctx, 10*time.Second)
		if w, av, err := fc.FuturesUSDTBalance(bctx); err != nil {
			log.Printf("баланс USDT-M: не удалось прочитать: %v", err)
		} else {
			log.Printf("фьючерсный USDT: кошелёк=%.4f доступно для ордеров=%.4f", w, av)
		}
		bcancel()
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
		peakNote := ""
		if aggNearPeakFrac > 0 {
			peakNote = fmt.Sprintf(", last у хая окна (≤%.3f%% от max)", aggNearPeakFrac*100)
		}
		lofNote := ""
		if aggMinLof > 0 {
			lofNote = fmt.Sprintf(", first→last ≥%.3f%%", aggMinLof)
		}
		qNote := ""
		if aggMinQualTr > 0 {
			qNote = fmt.Sprintf(", ≥%d тейкер-buy", aggMinQualTr)
		}
		liveBoost := ""
		if liveTrading {
			liveBoost = " [лайв: усиленные пороги]"
		}
		aggRule = fmt.Sprintf("окно %dms, ≥%.0fk USDT, min→last ≥%.3f%%%s%s%s, тейкер %s%s",
			aggWinMs, aggMinQ/1000, aggMinMv, peakNote, lofNote, qNote, taker, liveBoost)
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
	scratchStr := "выкл"
	if scratchDur > 0 {
		scratchStr = fmt.Sprintf("первые %s", scratchDur.String())
		if scratchMinDur > 0 {
			scratchStr += fmt.Sprintf(", не раньше %v в позиции", scratchMinDur)
		}
		scratchStr += fmt.Sprintf(", ниже входа на ≥%.2f%% без подтверждения", scratchPull*100)
	}
	flashStr := "выкл"
	if flashWindow > 0 {
		flashStr = fmt.Sprintf("окно %v: просадка от max в окне ≥%.2f%% или наклон ≥%.2f%% за ≥%v",
			flashWindow, flashDrop*100, flashSlope*100, flashMinSpan)
	}
	log.Printf("Выход по позиции: FLASH=%s | SCRATCH=%s | TRAIL: откат ≥%.2f%% от пика и net≥%.4f USDT (после комиссии в модели) | TP +%.4f%% | SL −%.4f%%",
		flashStr, scratchStr, trailBack*100, trailMinNet, tpMove*100, slMove*100)

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
				} else if h.live && h.fapi != nil {
					sctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
					w, av, err := h.fapi.FuturesUSDTBalance(sctx)
					cancel()
					blk := ""
					if h.entriesPaused.Load() {
						blk = " | ВХОДЫ ЗАБЛОКИРОВАНЫ (страховка)"
					}
					if err != nil {
						log.Printf("STATS: сделок=%d суммарный PnL=%.4f USDT [лайв]%s | баланс: %v",
							h.closedTrades.Load(), pnl, blk, err)
					} else {
						log.Printf("STATS: сделок=%d суммарный PnL=%.4f USDT [лайв]%s | USDT-M кошелёк=%.4f доступно=%.4f",
							h.closedTrades.Load(), pnl, blk, w, av)
					}
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
