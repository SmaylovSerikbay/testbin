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
	"net/url"
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

// Публичные klines (без ключа); отдельный таймаут от торгового REST.
var fapiPublicHTTP = &http.Client{Timeout: 8 * time.Second}

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
	mu               sync.Mutex
	lastPrice        float64
	haveLast         bool
	lastQuoteVol     float64 // накопленный quote volume (24h) с прошлого тика
	haveQuoteVol     bool
	inPos            bool
	entry            float64
	qty              float64
	notional         float64
	openedAt         time.Time
	pumpArmCnt       int
	lastSignalAt     time.Time
	opening          bool      // в процессе выставления лайв-ордера
	closing          bool      // в процессе закрытия лайв-ордера
	htfCooldownUntil time.Time // после отказа HTF не дергать klines/лог до момента (антиспам agg)
	posLev           int       // фактическое плечо лайв-позиции (0 = симуляция / не задано)
	peakPx           float64   // максимум цены в позиции (для трейлинга)
	trailArmed       bool      // был ли откат от пика после «настоящего» движения (с учётом комиссии)
	slBelowCnt       int       // подряд тиков с ценой ≤ линии SL (антимикрошум + меньше «ножа» на одном тике)

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

func validateHTFInterval(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	switch s {
	case "1m", "3m", "5m", "15m", "30m", "1h":
		return s
	default:
		return "1m"
	}
}

func klineParseFloat(v interface{}) (float64, error) {
	switch x := v.(type) {
	case string:
		return strconv.ParseFloat(x, 64)
	case float64:
		return x, nil
	case json.Number:
		return x.Float64()
	default:
		return 0, fmt.Errorf("kline: тип %T", v)
	}
}

func klineOpenClose(row []interface{}) (open, close float64, err error) {
	if len(row) < 5 {
		return 0, 0, fmt.Errorf("kline: короткая строка")
	}
	open, err = klineParseFloat(row[1])
	if err != nil {
		return 0, 0, err
	}
	close, err = klineParseFloat(row[4])
	return open, close, err
}

func fetchFuturesKlines(ctx context.Context, baseREST, symbol, interval string, limit int) ([][]interface{}, error) {
	base := strings.TrimSuffix(baseREST, "/")
	u := fmt.Sprintf("%s/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
		base, url.QueryEscape(symbol), url.QueryEscape(interval), limit)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := fapiPublicHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		sn := len(b)
		if sn > 300 {
			sn = 300
		}
		return nil, fmt.Errorf("klines HTTP %d: %s", resp.StatusCode, string(b[:sn]))
	}
	var rows [][]interface{}
	if err := json.Unmarshal(b, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (h *hub) checkHTFLong(ctx context.Context, sym string) (ok bool, reason string) {
	if h.htfFilter <= 0 || h.restBase == "" {
		return true, ""
	}
	limit := 4
	if h.htfFilter >= 2 {
		limit = 5
	}
	rows, err := fetchFuturesKlines(ctx, h.restBase, sym, h.htfInterval, limit)
	if err != nil {
		return false, err.Error()
	}
	if len(rows) < 3 {
		return false, fmt.Sprintf("мало свечей (%d)", len(rows))
	}
	n := len(rows)
	o1, c1, err := klineOpenClose(rows[n-2])
	if err != nil {
		return false, err.Error()
	}
	_, c0, err := klineOpenClose(rows[n-3])
	if err != nil {
		return false, err.Error()
	}
	switch h.htfFilter {
	case 1:
		if c1 <= o1 {
			return false, fmt.Sprintf("%s: последняя закрытая не бычья (C≤O)", h.htfInterval)
		}
	case 2:
		if c1 <= c0 {
			return false, fmt.Sprintf("%s: close ≤ предыдущего close", h.htfInterval)
		}
	case 3:
		if c1 <= o1 || c1 <= c0 {
			return false, fmt.Sprintf("%s: нужны бычья + рост close", h.htfInterval)
		}
	default:
		return true, ""
	}
	return true, ""
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
	sym = normalizeFuturesSym(sym)
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
	sym = normalizeFuturesSym(sym)
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

type markExitSnap struct {
	px float64
	at time.Time
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

	// За интервал STATS: сколько раз agg прошёл все фильтры и запущен лайв-вход (до HTF/ордера).
	aggPassLive atomic.Uint64
	// Диагностика отсева agg (Swap в STATS).
	aggRejQuote  atomic.Uint64 // сумма USDT в окне < порога
	aggRejMove   atomic.Uint64 // min→last % или last на минимуме окна
	aggRejHL     atomic.Uint64
	aggRejPeak   atomic.Uint64
	aggRejQual   atomic.Uint64
	aggRejLOF    atomic.Uint64
	aggRejSingle atomic.Uint64
	aggRejVwap   atomic.Uint64
	aggRejHtfWait atomic.Uint64 // лайв: все agg ок, но символ в паузе после отказа HTF

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
	aggMinSingleTradeUSDT  float64 // 0 = выкл; max одной квалиф. сделки в USDT ≥ порога («крупный тик»)
	aggMinHighLowRangePct  float64 // 0 = выкл; (max−min)/min в окне в % — отсекаем узкий 2–3% дребезг
	// Оконный VWAP по квалиф. сделкам: last ≥ VWAP на N% (идея из VWAP/объёмных скальпинг-фильтров, меньше «пустых» тиков).
	aggMinLastOverVwapPct float64

	// Старший таймфрейм по REST klines (публично): направление последних закрытых свечей перед лонгом.
	restBase        string
	htfFilter       int           // 0=выкл; 1=последняя закрытая бычья (C>O); 2=close>prev close; 3=оба
	htfInterval     string        // 1m, 5m, …
	htfFailCooldown time.Duration // пауза на символ после отказа HTF (меньше REST и строк в логе)

	// «пропуск HTF» не чаще одного раза за интервал (на символ), даже если кулдаун на symState сбился.
	htfSkipLogMu sync.Mutex
	htfSkipLogAt map[string]time.Time

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
	// В лайве: TP/SL/CAP/CUT считать по mark (как в приложении Binance), а не по last из miniTicker — иначе «−11%» по марже при «тихом» last.
	exitUseMark    bool
	markExitCache  sync.Map // string -> markExitSnap

	// выход «под памп»: быстрый scratch, трейлинг от пика, комиссия в пороге трейла
	scratchDur       time.Duration // 0 = выкл
	scratchMinDur    time.Duration // не раньше N в позиции (антимикрошум)
	scratchPull      float64       // доля цены: ниже входа на столько — «не угадали памп»
	trailBack        float64       // откат от peakPx для фиксации
	trailBackTight   float64       // 0 = выкл; уже откат, если net уже в хорошем плюсе (фиксация жадности)
	trailTightMinNet float64       // порог net USDT для включения trailBackTight (0 → как trailMinNetUSDT)
	trailFeeExtra    float64       // сверх оценки round-trip комиссии для включения трейла (доля)
	trailMinNetUSDT  float64       // TRAIL только если оценка net PnL ≥ этого (не фиксировать «трейл» в минус)
	bankNetUSDT      float64       // 0 = выкл; закрыть лонг при net ≥ (зафиксировать плюс, не ждать TP)
	bankMarginPct    float64       // 0 = выкл; то же в % от маржи (как «% на $1» в логе), срабатывает раньше жадного TP
	bankMinHold      time.Duration // мин. время в позиции перед BANK (0 = сразу)

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

	slConfirmTicks int           // SL только после N подряд тиков ≤ линии стопа (1 = как раньше)
	cutMarginPct   float64       // 0 = выкл; выход при ROI на маржу ≤ −X% (модель), быстрее чем SL подряд N тиков
	cutMinHold     time.Duration // не CUT на первых секундах (модель ≈ −fee при цене у входа)
	cutNeedBelow   bool          // true: CUT только если цена < входа (не резать «зелёный» тик из‑за комиссии)
	maxLossUSDT    float64       // 0 = выкл; net в модели ≤ −X USDT — CAP (раньше глубокого SL/fill на альтах)
	capMinHold     time.Duration // не CAP на первых тиках (модель сразу минус на комиссии round-trip)
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

// htfSymCooldownActive — символ в паузе после отказа HTF.
// Пока until == time.Time{} (ноль), паузы нет: иначе !now.Before(until) при нулевом until
// всегда истинно (текущее время не раньше года 1), и лайв/сим входы блокируются навсегда.
func htfSymCooldownActive(now, until time.Time, failCD time.Duration) bool {
	return failCD > 0 && !until.IsZero() && !now.Before(until)
}

// liveSymCooldownDur — длительность паузы символа после отказа HTF / ошибки подготовки qty.
// Если в env 0 — минимум 2s (иначе шторм REST и сотни строк «пропуск HTF» в секунду).
func (h *hub) liveSymCooldownDur() time.Duration {
	if h.htfFailCooldown > 0 {
		return h.htfFailCooldown
	}
	return 2 * time.Second
}

func normalizeFuturesSym(sym string) string {
	return strings.ToUpper(strings.TrimSpace(sym))
}

func (h *hub) logHTFReject(sym, why string, sim bool) {
	gap := h.liveSymCooldownDur()
	h.htfSkipLogMu.Lock()
	if h.htfSkipLogAt == nil {
		h.htfSkipLogAt = make(map[string]time.Time)
	}
	now := time.Now()
	if t, ok := h.htfSkipLogAt[sym]; ok && now.Sub(t) < gap {
		h.htfSkipLogMu.Unlock()
		return
	}
	h.htfSkipLogAt[sym] = now
	h.htfSkipLogMu.Unlock()
	if sim {
		log.Printf("[%s] симуляция: пропуск HTF: %s", sym, why)
	} else {
		log.Printf("[%s] пропуск HTF: %s", sym, why)
	}
}

// markPriceForExit — mark для расчёта выходов в лайве (кэш ~400ms, иначе REST на каждый тик).
func (h *hub) markPriceForExit(sym string, last float64) float64 {
	if h.fapi == nil || !h.exitUseMark {
		return last
	}
	const ttl = 400 * time.Millisecond
	now := time.Now()
	if v, ok := h.markExitCache.Load(sym); ok {
		s := v.(markExitSnap)
		if now.Sub(s.at) < ttl && s.px > 0 {
			return s.px
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	m, err := h.fapi.MarkPrice(ctx, sym)
	cancel()
	if err != nil || m <= 0 {
		return last
	}
	h.markExitCache.Store(sym, markExitSnap{px: m, at: now})
	return m
}

func (h *hub) processTick(sym string, price, quoteVol float64, haveQ bool, now time.Time) {
	sym = normalizeFuturesSym(sym)
	v, _ := h.states.LoadOrStore(sym, newSymState(h.notionalUSDT, h.histLen))
	st := v.(*symState)

	st.mu.Lock()
	defer st.mu.Unlock()

	if st.inPos {
		if h.live && st.closing {
			return
		}
		pxEval := price
		if h.live && h.exitUseMark {
			st.mu.Unlock()
			pxEval = h.markPriceForExit(sym, price)
			st.mu.Lock()
			if !st.inPos {
				return
			}
			if h.live && st.closing {
				return
			}
		}
		levM := float64(h.levInt)
		if st.posLev > 0 {
			levM = float64(st.posLev)
		}
		tpMove := priceMoveForMarginPct(h.tpMarginPct, levM)
		slMove := priceMoveForMarginPct(h.slMarginPct, levM)
		tpFrac := 1 + tpMove

		if pxEval > st.peakPx {
			st.peakPx = pxEval
		}
		var feeFrac float64
		if st.qty > 0 && st.entry > 0 {
			feeFrac = (h.feeRT * st.notional) / (st.qty * st.entry)
		}
		trailOnFrac := feeFrac + h.trailFeeExtra
		if st.peakPx >= st.entry*(1+trailOnFrac) {
			st.trailArmed = true
		}

		var netEst float64
		if st.qty > 0 && st.entry > 0 {
			netEst = st.qty*(pxEval-st.entry) - h.feeRT*st.notional
		}

		h.pushFlashSample(st, now, pxEval)
		if h.checkFlashDump(st, pxEval, now) {
			h.triggerClose(st, sym, "FLASH", pxEval, now)
			return
		}
		if h.maxLossUSDT > 0 && netEst <= -h.maxLossUSDT &&
			(h.capMinHold <= 0 || now.Sub(st.openedAt) >= h.capMinHold) {
			st.slBelowCnt = 0
			h.triggerClose(st, sym, "CAP", pxEval, now)
			return
		}

		// TP/SL до TIME: таймер не должен «перебивать» жёсткий стоп; убыток в % на маржу = движение цены × плечо.
		if pxEval >= st.entry*tpFrac {
			st.slBelowCnt = 0
			h.triggerClose(st, sym, "TP", pxEval, now)
			return
		}
		slLine := st.entry * (1 - slMove)
		if h.slMarginPct > 0 && st.entry > 0 {
			if pxEval <= slLine {
				st.slBelowCnt++
				need := h.slConfirmTicks
				if need < 1 {
					need = 1
				}
				if st.slBelowCnt >= need {
					st.slBelowCnt = 0
					h.triggerClose(st, sym, "SL", pxEval, now)
					return
				}
			} else {
				st.slBelowCnt = 0
			}
		}
		// Фиксация плюса: либо net USDT, либо % на маржу — после bankMinHold.
		// Буфер: netEst уже с RT-комиссией в модели, но MARKET на закрытии даёт проскальзывание — без буфера бывают
		// «BANK» по краткому движению mark, а fill около входа → в логе минус (как entry=exit при BANK).
		bankHoldOK := h.bankMinHold <= 0 || now.Sub(st.openedAt) >= h.bankMinHold
		if bankHoldOK {
			bankBuf := 0.005 + h.feeRT*st.notional
			if bankBuf < 0.008 {
				bankBuf = 0.008
			}
			effMar := h.marginUSDT
			if st.posLev > 0 && st.notional > 0 {
				if em := st.notional / float64(st.posLev); em > 1e-9 {
					effMar = em
				}
			}
			bankHit := (h.bankNetUSDT > 0 && netEst >= h.bankNetUSDT+bankBuf) ||
				(h.bankMarginPct > 0 && effMar > 1e-12 && netEst >= bankBuf && netEst/effMar*100 >= h.bankMarginPct)
			if bankHit {
				h.triggerClose(st, sym, "BANK", pxEval, now)
				return
			}
		}
		if h.scratchDur > 0 && now.Sub(st.openedAt) < h.scratchDur &&
			(h.scratchMinDur <= 0 || now.Sub(st.openedAt) >= h.scratchMinDur) &&
			!st.trailArmed && st.peakPx < st.entry*(1+trailOnFrac) && pxEval <= st.entry*(1-h.scratchPull) {
			h.triggerClose(st, sym, "SCRATCH", pxEval, now)
			return
		}
		trailEff := h.trailBack
		if h.trailBackTight > 0 && h.trailBackTight < trailEff {
			tightMin := h.trailTightMinNet
			if tightMin <= 0 {
				tightMin = h.trailMinNetUSDT
			}
			if netEst >= tightMin {
				trailEff = h.trailBackTight
			}
		}
		if st.trailArmed && st.peakPx > 0 && pxEval <= st.peakPx*(1-trailEff) {
			if netEst >= h.trailMinNetUSDT {
				h.triggerClose(st, sym, "TRAIL", pxEval, now)
				return
			}
		}
		// Медленный слив: SL ждёт N тиков подряд; CUT — только реальное проседание цены (не один шум + комиссия).
		cutHoldOK := h.cutMinHold <= 0 || now.Sub(st.openedAt) >= h.cutMinHold
		cutDirOK := !h.cutNeedBelow || pxEval < st.entry
		if h.cutMarginPct > 0 && h.marginUSDT > 1e-12 && netEst < 0 && cutHoldOK && cutDirOK {
			if netEst/h.marginUSDT*100 <= -h.cutMarginPct {
				st.slBelowCnt = 0
				h.triggerClose(st, sym, "CUT", pxEval, now)
				return
			}
		}
		if h.maxHold > 0 && now.Sub(st.openedAt) >= h.maxHold {
			// Уже за линией SL, но не хватило второго тика — не метить TIME (хуже смысл и тот же MARKET).
			if h.slMarginPct > 0 && st.entry > 0 && pxEval <= slLine {
				st.slBelowCnt = 0
				h.triggerClose(st, sym, "SL", pxEval, now)
				return
			}
			h.triggerClose(st, sym, "TIME", pxEval, now)
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
		if h.live {
			if h.entriesPaused.Load() || st.opening || st.inPos {
				return
			}
			if htfSymCooldownActive(now, st.htfCooldownUntil, h.liveSymCooldownDur()) {
				return
			}
			st.opening = true
			go h.doOpenAsync(sym, st, price, now)
			return
		}
		if st.opening || st.inPos {
			return
		}
		if h.htfFilter > 0 {
			if htfSymCooldownActive(now, st.htfCooldownUntil, h.liveSymCooldownDur()) {
				return
			}
			st.opening = true
			go h.doOpenSimAfterHTF(sym, st, price, now)
			return
		}
		st.lastSignalAt = now
		st.openSim(price, now)
	}
}

func (h *hub) processAgg(sym string, price, qty float64, buyerMaker bool, tradeMs int64, now time.Time) {
	if !h.aggEnabled {
		return
	}
	sym = normalizeFuturesSym(sym)
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

	var minP, maxP, lastQual, firstQual, sumQ, maxSingleQ, baseVolSum float64
	var have bool
	qualN := 0
	for j := range st.aggBuf {
		pt := &st.aggBuf[j]
		if h.aggTakerBuy && pt.buyerMaker {
			continue
		}
		qualN++
		sumQ += pt.quoteUSDT
		if pt.price > 0 {
			baseVolSum += pt.quoteUSDT / pt.price
		}
		if pt.quoteUSDT > maxSingleQ {
			maxSingleQ = pt.quoteUSDT
		}
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
	if sumQ < h.aggMinQuote {
		h.aggRejQuote.Add(1)
		return
	}
	if movePct < h.aggMinMovePct || lastQual <= minP {
		h.aggRejMove.Add(1)
		return
	}
	hlRangePct := (maxP - minP) / minP * 100
	if h.aggMinHighLowRangePct > 0 && hlRangePct < h.aggMinHighLowRangePct {
		h.aggRejHL.Add(1)
		return
	}
	if h.aggNearPeakFrac > 0 && lastQual < maxP*(1-h.aggNearPeakFrac) {
		h.aggRejPeak.Add(1)
		return
	}
	if h.aggMinQualTrades > 0 && qualN < h.aggMinQualTrades {
		h.aggRejQual.Add(1)
		return
	}
	if h.aggMinLastOverFirstPct > 0 && firstQual > 0 {
		lof := (lastQual - firstQual) / firstQual * 100
		if lof < h.aggMinLastOverFirstPct {
			h.aggRejLOF.Add(1)
			return
		}
	}
	if h.aggMinSingleTradeUSDT > 0 && maxSingleQ < h.aggMinSingleTradeUSDT {
		h.aggRejSingle.Add(1)
		return
	}
	if h.aggMinLastOverVwapPct > 0 && baseVolSum > 0 && lastQual > 0 {
		vwap := sumQ / baseVolSum
		if vwap <= 0 {
			h.aggRejVwap.Add(1)
			return
		}
		overVwapPct := (lastQual - vwap) / vwap * 100
		if overVwapPct < h.aggMinLastOverVwapPct {
			h.aggRejVwap.Add(1)
			return
		}
	}
	if h.live {
		if h.entriesPaused.Load() || st.opening || st.inPos {
			return
		}
		if htfSymCooldownActive(now, st.htfCooldownUntil, h.liveSymCooldownDur()) {
			h.aggRejHtfWait.Add(1)
			return
		}
		h.aggPassLive.Add(1)
		st.opening = true
		go h.doOpenAsync(sym, st, lastQual, now)
		return
	}
	if st.opening || st.inPos {
		return
	}
	if h.htfFilter > 0 {
		if htfSymCooldownActive(now, st.htfCooldownUntil, h.liveSymCooldownDur()) {
			return
		}
		st.opening = true
		go h.doOpenSimAfterHTF(sym, st, lastQual, now)
		return
	}
	st.lastSignalAt = now
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
	st.slBelowCnt = 0
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
	displayReason := reason
	// Лонг+SL по мини-тикеру, MARKET fill мог оказаться выше входа (лага/волатильность) — не путать с ошибкой логики.
	if reason == "SL" && pnl > 1e-8 && st.entry > 0 && exitPrice > st.entry {
		displayReason = "SL (тик≤стоп, fill выше входа)"
	}
	log.Printf("[%s] %s entry=%.8f exit=%.8f pnl=%.6f USDT (%.2f%% на $1) hold=%s [%s]",
		sym, displayReason, st.entry, exitPrice, pnl, roi, hold.Truncate(time.Second), mode)
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

func (h *hub) doOpenSimAfterHTF(sym string, st *symState, ref float64, now time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ok, why := h.checkHTFLong(ctx, sym)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.opening = false
	if !ok {
		st.htfCooldownUntil = time.Now().Add(h.liveSymCooldownDur())
		h.logHTFReject(sym, why, true)
		return
	}
	if st.inPos {
		return
	}
	st.lastSignalAt = now
	st.openSim(ref, now)
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

	if h.htfFilter > 0 {
		if ok, why := h.checkHTFLong(ctx, sym); !ok {
			st.mu.Lock()
			st.opening = false
			st.htfCooldownUntil = time.Now().Add(h.liveSymCooldownDur())
			st.mu.Unlock()
			h.logHTFReject(sym, why, false)
			return
		}
	}
	st.mu.Lock()
	st.lastSignalAt = now
	st.mu.Unlock()

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
	qtyStr, _, err := h.fapi.PrepareQtyBuyLive(ctx, sym, effNotional, refPrice)
	if err != nil {
		h.restMu.Unlock()
		st.mu.Lock()
		st.opening = false
		st.htfCooldownUntil = time.Now().Add(h.liveSymCooldownDur())
		st.mu.Unlock()
		log.Printf("[%s] подготовка qty: %v", sym, err)
		return
	}
	avg, q, err := h.fapi.MarketBuyLong(ctx, sym, qtyStr, h.liveHedge)
	if err != nil && (strings.Contains(err.Error(), "-4164") || strings.Contains(err.Error(), "notional must be no smaller")) {
		if q2, berr := h.fapi.BumpQtyAfter4164(ctx, sym, qtyStr, effNotional); berr == nil {
			avg, q, err = h.fapi.MarketBuyLong(ctx, sym, q2, h.liveHedge)
			if err == nil {
				qtyStr = q2
			}
		}
	}
	h.restMu.Unlock()

	st.mu.Lock()
	defer st.mu.Unlock()
	st.opening = false
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "-4164") || strings.Contains(msg, "notional must be no smaller") {
			log.Printf("[%s] market BUY: %v (редко после PrepareQtyBuyLive — проверьте лимиты пары / нотиoнал)", sym, err)
		} else if strings.Contains(msg, "-2019") || strings.Contains(msg, "insufficient") {
			log.Printf("[%s] market BUY: %v (мало доступной маржи под IM — снизьте LIVE_MARGIN_BUFFER_PCT или MARGIN_USDT)", sym, err)
		} else {
			log.Printf("[%s] market BUY: %v", sym, err)
		}
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
	st.slBelowCnt = 0
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
	cutMg := envGetFloat(em, "PUMP_CUT_MARGIN_PCT", 0)
	if cutMg < 0 {
		cutMg = 0
	}
	cutMinSec := 0
	if strings.TrimSpace(envGet(em, "PUMP_CUT_MIN_SEC", "")) != "" {
		cutMinSec = envGetInt(em, "PUMP_CUT_MIN_SEC", 0)
	} else if cutMg > 0 {
		cutMinSec = 4
	}
	if cutMinSec < 0 {
		cutMinSec = 0
	}
	var cutMinHold time.Duration
	if cutMinSec > 0 {
		cutMinHold = time.Duration(cutMinSec) * time.Second
	}
	cutNeedBelow := strings.TrimSpace(envGet(em, "PUMP_CUT_REQUIRE_BELOW_ENTRY", "1")) != "0"
	maxLossU := envGetFloat(em, "PUMP_MAX_LOSS_USDT", 0)
	if maxLossU < 0 {
		maxLossU = 0
	}
	capMinSec := 0
	if strings.TrimSpace(envGet(em, "PUMP_CAP_MIN_SEC", "")) != "" {
		capMinSec = envGetInt(em, "PUMP_CAP_MIN_SEC", 0)
	} else if maxLossU > 0 {
		capMinSec = 4 // иначе первый тик: net≈−fee и ложный CAP при шуме
	}
	if capMinSec < 0 {
		capMinSec = 0
	}
	var capMinHold time.Duration
	if capMinSec > 0 {
		capMinHold = time.Duration(capMinSec) * time.Second
	}

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
	liveTrading := strings.TrimSpace(envGet(em, "LIVE_TRADING", "")) == "1"
	if liveTrading {
		pticks += envGetInt(em, "LIVE_PUMP_CONFIRM_TICKS_ADD", 0)
		if pticks > 25 {
			pticks = 25
		}
	}
	var slConfirm int
	if strings.TrimSpace(envGet(em, "SL_CONFIRM_TICKS", "")) != "" {
		slConfirm = envGetInt(em, "SL_CONFIRM_TICKS", 1)
	} else if liveTrading {
		slConfirm = 2
	} else {
		slConfirm = 1
	}
	if slConfirm < 1 {
		slConfirm = 1
	}
	if slConfirm > 15 {
		slConfirm = 15
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
	aggMinSingle := envGetFloat(em, "AGG_MIN_SINGLE_TRADE_USDT", 0)
	if aggMinSingle < 0 {
		aggMinSingle = 0
	}
	if liveTrading && aggMinSingle > 0 {
		if msm := envGetFloat(em, "LIVE_AGG_SINGLE_TRADE_MULT", 1); msm > 1 {
			aggMinSingle *= msm
		}
	}
	aggMinHlRg := envGetFloat(em, "AGG_MIN_HIGH_LOW_RANGE_PCT", 0)
	if aggMinHlRg < 0 {
		aggMinHlRg = 0
	}
	// Если база 0 — фильтр выкл; не добавлять LIVE add (иначе «0» в .env всё равно даёт ~0.008%).
	if liveTrading && aggMinHlRg > 0 {
		aggMinHlRg += envGetFloat(em, "LIVE_AGG_HIGH_LOW_ADD_PCT", 0)
	}
	if aggMinHlRg < 0 {
		aggMinHlRg = 0
	}
	aggVwapMin := envGetFloat(em, "AGG_MIN_LAST_OVER_VWAP_PCT", 0)
	if aggVwapMin < 0 {
		aggVwapMin = 0
	}
	if liveTrading && aggVwapMin > 0 {
		aggVwapMin += envGetFloat(em, "LIVE_AGG_VWAP_ADD_PCT", 0)
	}
	if aggVwapMin < 0 {
		aggVwapMin = 0
	}
	aggTaker := strings.TrimSpace(envGet(em, "AGG_TAKER_BUY_ONLY", "1")) != "0"

	htfF := envGetInt(em, "PUMP_HTF_FILTER", 0)
	if htfF < 0 || htfF > 3 {
		htfF = 0
	}
	htfIv := validateHTFInterval(envGet(em, "PUMP_HTF_INTERVAL", "1m"))
	htfFailSec := envGetInt(em, "PUMP_HTF_FAIL_COOLDOWN_SEC", 12)
	if htfFailSec < 0 {
		htfFailSec = 0
	}
	var htfFailCooldown time.Duration
	if htfFailSec > 0 {
		htfFailCooldown = time.Duration(htfFailSec) * time.Second
	}

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
	trailBackTight := envGetFloat(em, "PUMP_TRAIL_BACK_TIGHT_PCT", 0) / 100
	if trailBackTight < 0 {
		trailBackTight = 0
	}
	trailTightMinNet := envGetFloat(em, "PUMP_TRAIL_TIGHT_MIN_NET_USDT", 0)
	if trailTightMinNet < 0 {
		trailTightMinNet = 0
	}
	bankNet := envGetFloat(em, "PUMP_BANK_NET_USDT", 0)
	if bankNet < 0 {
		bankNet = 0
	}
	bankMarginPct := envGetFloat(em, "PUMP_BANK_MARGIN_PCT", 0)
	if bankMarginPct < 0 {
		bankMarginPct = 0
	}
	bankMinSec := envGetInt(em, "PUMP_BANK_MIN_SEC", 0)
	var bankMinHold time.Duration
	if bankMinSec > 0 {
		bankMinHold = time.Duration(bankMinSec) * time.Second
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

	exitUseMark := false
	if liveTrading {
		exitUseMark = strings.TrimSpace(envGet(em, "LIVE_EXIT_USE_MARK", "1")) != "0"
	}

	h := &hub{
		restBase:               baseREST,
		htfFilter:              htfF,
		htfInterval:            htfIv,
		htfFailCooldown:        htfFailCooldown,
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
		aggMinSingleTradeUSDT:  aggMinSingle,
		aggMinHighLowRangePct:  aggMinHlRg,
		aggMinLastOverVwapPct:  aggVwapMin,
		maxHold:                maxHold,
		live:                   false,
		liveHedge:              liveHedge,
		liveMaxPos:             liveMaxPos,
		levInt:                 levInt,
		liveMarginFrac:         liveMarginFrac,
		exitUseMark:            exitUseMark,
		scratchDur:             scratchDur,
		scratchMinDur:          scratchMinDur,
		scratchPull:            scratchPull,
		trailBack:              trailBack,
		trailBackTight:         trailBackTight,
		trailTightMinNet:       trailTightMinNet,
		trailFeeExtra:          trailFeeExtra,
		trailMinNetUSDT:        trailMinNet,
		bankNetUSDT:            bankNet,
		bankMarginPct:          bankMarginPct,
		bankMinHold:            bankMinHold,
		flashWindow:            flashWindow,
		flashDropFrac:          flashDrop,
		flashSlopeFrac:         flashSlope,
		flashMinSpan:           flashMinSpan,
		maxSessionLossUSDT:     maxSessLoss,
		minAvailToTrade:        minAvailTrade,
		maxConsecLosses:        maxConsecLoss,
		slConfirmTicks:         slConfirm,
		cutMarginPct:           cutMg,
		cutMinHold:             cutMinHold,
		cutNeedBelow:           cutNeedBelow,
		maxLossUSDT:            maxLossU,
		capMinHold:             capMinHold,
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
		singleNote := ""
		if aggMinSingle > 0 {
			singleNote = fmt.Sprintf(", max-тик ≥%.0f USDT", aggMinSingle)
		}
		hlNote := ""
		if aggMinHlRg > 0 {
			hlNote = fmt.Sprintf(", размах (max−min)/min ≥%.3f%%", aggMinHlRg)
		}
		vwapNote := ""
		if aggVwapMin > 0 {
			vwapNote = fmt.Sprintf(", last≥VWAP окна +%.3f%%", aggVwapMin)
		}
		liveBoost := ""
		if liveTrading {
			liveBoost = " [лайв: усиленные пороги]"
		}
		aggRule = fmt.Sprintf("окно %dms, ≥%.0fk USDT, min→last ≥%.3f%%%s%s%s%s%s%s, тейкер %s%s",
			aggWinMs, aggMinQ/1000, aggMinMv, peakNote, hlNote, vwapNote, lofNote, qNote, singleNote, taker, liveBoost)
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
	htfNote := "HTF выкл"
	if htfF == 1 {
		htfNote = fmt.Sprintf("HTF %s: последняя закрытая бычья (C>O)", htfIv)
	} else if htfF == 2 {
		htfNote = fmt.Sprintf("HTF %s: close > prev close", htfIv)
	} else if htfF == 3 {
		htfNote = fmt.Sprintf("HTF %s: бычья + close↑ (оба)", htfIv)
	}
	if htfF > 0 {
		effCD := htfFailCooldown
		if effCD <= 0 {
			effCD = 2 * time.Second
		}
		htfNote += fmt.Sprintf("; пауза символа после отказа %v", effCD.Truncate(time.Second))
	}
	log.Printf("WS: %s — mini conn=%d agg conn=%d | вход agg: %s | вход mini: %s | мини памп: цена ≥ %s; Δq %s; окно=%d разгон=%d max=%.1f%% подряд=%d cd=%s | max_hold=%s | %s | %s $%.2f ×%.0f TP=%.0f%% SL=%.0f%% (цена +%.4f%% / −%.4f%%) | SL подряд %d тик(а)",
		baseWS, len(batches), aggConnN, aggRule, miniRule, priceRule, volRule, histLen, warmup, maxPump, pticks, cd, maxHoldStr, htfNote, modeStr, m, lev, tpm, slm, tpMove*100, slMove*100, slConfirm)
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
	bankStr := "выкл"
	if bankNet > 0 || bankMarginPct > 0 {
		switch {
		case bankNet > 0 && bankMarginPct > 0:
			bankStr = fmt.Sprintf("net≥%.4f USDT или ROI на маржу ≥%.1f%%", bankNet, bankMarginPct)
		case bankNet > 0:
			bankStr = fmt.Sprintf("net≥%.4f USDT", bankNet)
		default:
			bankStr = fmt.Sprintf("ROI на маржу ≥%.1f%%", bankMarginPct)
		}
		if bankMinHold > 0 {
			bankStr += fmt.Sprintf(", не раньше %v в позиции", bankMinHold.Truncate(time.Second))
		}
	}
	trailTightNote := ""
	if trailBackTight > 0 {
		tnm := trailTightMinNet
		if tnm <= 0 {
			tnm = trailMinNet
		}
		trailTightNote = fmt.Sprintf("; при net≥%.4f → откат ≥%.2f%%", tnm, trailBackTight*100)
	}
	cutStr := "выкл"
	if cutMg > 0 {
		cutStr = fmt.Sprintf("ROI≤−%.1f%% на маржу (модель)", cutMg)
		if cutNeedBelow {
			cutStr += ", цена<вход"
		}
		if cutMinHold > 0 {
			cutStr += fmt.Sprintf(", не раньше %v", cutMinHold.Truncate(time.Second))
		}
	}
	capStr := "выкл"
	if maxLossU > 0 {
		if capMinHold > 0 {
			capStr = fmt.Sprintf("net≤−%.4f USDT (модель), не раньше %v в позиции", maxLossU, capMinHold.Truncate(time.Second))
		} else {
			capStr = fmt.Sprintf("net≤−%.4f USDT (модель)", maxLossU)
		}
	}
	exitMarkNote := ""
	if liveTrading && exitUseMark {
		exitMarkNote = " | лайв: выходы по mark (как PnL в приложении; кэш ~400ms)"
	}
	log.Printf("Выход по позиции: CAP=%s | CUT=%s | BANK=%s | FLASH=%s | SCRATCH=%s | TRAIL: откат ≥%.2f%%%s, иначе net≥%.4f | TP +%.4f%% | SL −%.4f%%; TIME+цена≤SL→SL%s",
		capStr, cutStr, bankStr, flashStr, scratchStr, trailBack*100, trailTightNote, trailMinNet, tpMove*100, slMove*100, exitMarkNote)

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
					ap := h.aggPassLive.Swap(0)
					rq := h.aggRejQuote.Swap(0)
					rm := h.aggRejMove.Swap(0)
					rhl := h.aggRejHL.Swap(0)
					rpk := h.aggRejPeak.Swap(0)
					rqual := h.aggRejQual.Swap(0)
					rlf := h.aggRejLOF.Swap(0)
					rs := h.aggRejSingle.Swap(0)
					rvw := h.aggRejVwap.Swap(0)
					rht := h.aggRejHtfWait.Swap(0)
					aggBlk := fmt.Sprintf(" | agg→лайв: %d | отсев quote=%d move=%d HL=%d peak=%d qual=%d 1st→last=%d single=%d vwap=%d htf_wait=%d",
						ap, rq, rm, rhl, rpk, rqual, rlf, rs, rvw, rht)
					if err != nil {
						log.Printf("STATS: сделок=%d суммарный PnL=%.4f USDT [лайв]%s%s | баланс: %v",
							h.closedTrades.Load(), pnl, blk, aggBlk, err)
					} else {
						log.Printf("STATS: сделок=%d суммарный PnL=%.4f USDT [лайв]%s%s | USDT-M кошелёк=%.4f доступно=%.4f",
							h.closedTrades.Load(), pnl, blk, aggBlk, w, av)
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
