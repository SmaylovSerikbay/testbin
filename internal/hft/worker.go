package hft

import (
	"context"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

const (
	minTPS           = 30
	move10sMin       = 0.001 // 0.1%
	staleOffWatch    = 45 * time.Second
	buySellRatio     = 3.0
	priceUp2s        = 0.0005 // 0.05%
	wallUSDT         = 100_000.0
	wallWindow       = 500 * time.Millisecond
	bbPeriod         = 10
	bbInterval       = 5 * time.Second
	entryDebounce    = 900 * time.Millisecond
	topDepthLevels   = 5
	highTPSBreakout  = 30
)

// SymbolWorker — один символ: aggTrade + depth20, фильтр «жара», триггеры.
type SymbolWorker struct {
	Symbol string
	Client *futures.Client
	Qty    string
	Stats  *SessionStats
	Prices *PriceHub

	mu            sync.Mutex
	tradeMs       []int64
	pricePts      []pricePoint
	buyLegs       []flowLeg
	sellLegs      []flowLeg
	lastHot       time.Time
	lastPrice     float64
	bbCloses      []float64
	lastBarTime   time.Time
	wallSnaps     []wallSnap
	lastEntryNano atomic.Int64
}

type pricePoint struct {
	ms    int64
	price float64
}

type wallSnap struct {
	t       time.Time
	askUSDT float64
	bidUSDT float64
	mid     float64
}

type flowLeg struct {
	ms int64
	q  float64
}

func NewSymbolWorker(sym string, c *futures.Client, qty string, st *SessionStats, ph *PriceHub) *SymbolWorker {
	return &SymbolWorker{
		Symbol:      sym,
		Client:      c,
		Qty:         qty,
		Stats:       st,
		Prices:      ph,
		lastHot:     time.Now(),
		lastBarTime: time.Now(),
	}
}

func (w *SymbolWorker) Run(ctx context.Context, hotOut *atomic.Bool) {
	errH := func(err error) {
		if err != nil && ctx.Err() == nil {
			w.Stats.PushImpulse(w.Symbol + " WS_ERR " + err.Error())
		}
	}

	doneA, stopA, err := futures.WsAggTradeServe(w.Symbol, w.onAggTrade, errH)
	if err != nil {
		errH(err)
		return
	}
	doneD, stopD, err := futures.WsPartialDepthServeWithRate(w.Symbol, 20, 100*time.Millisecond, w.onDepth, errH)
	if err != nil {
		close(stopA)
		errH(err)
		return
	}

	go func() {
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				w.tickStrategy(hotOut)
			}
		}
	}()

	select {
	case <-ctx.Done():
	case <-doneA:
	case <-doneD:
	}
	close(stopA)
	close(stopD)
}

func (w *SymbolWorker) onAggTrade(ev *futures.WsAggTradeEvent) {
	if ev == nil {
		return
	}
	px, _ := strconv.ParseFloat(ev.Price, 64)
	qty, _ := strconv.ParseFloat(ev.Quantity, 64)
	ms := ev.TradeTime
	if ms == 0 {
		ms = ev.Time
	}
	quote := px * qty

	w.mu.Lock()
	defer w.mu.Unlock()

	w.tradeMs = append(w.tradeMs, ms)
	w.trimMs(ms-2500, &w.tradeMs)

	w.pricePts = append(w.pricePts, pricePoint{ms: ms, price: px})
	w.trimPts(ms - 12_000)

	w.lastPrice = px
	w.Prices.Set(w.Symbol, px)

	if ev.Maker {
		w.sellLegs = append(w.sellLegs, flowLeg{ms: ms, q: quote})
	} else {
		w.buyLegs = append(w.buyLegs, flowLeg{ms: ms, q: quote})
	}
	w.trimFlow(&w.buyLegs, ms-2100)
	w.trimFlow(&w.sellLegs, ms-2100)

	tps := w.tpsLocked(ms)
	move10 := w.move10sLocked(ms)
	if tps >= minTPS && math.Abs(move10) >= move10sMin {
		w.lastHot = time.Now()
	}
}

func (w *SymbolWorker) trimVol2s(nowMs int64) {
	cut := nowMs - 2000
	// объёмы накапливаются только за последние события — сброс при устаревании: пересчёт с нуля дорогой,
	// используем экспоненциальное устаревание через метку
	_ = cut
}

// onDepth вызывается из WS; top5 notionals + стена.
func (w *SymbolWorker) onDepth(ev *futures.WsDepthEvent) {
	if ev == nil || len(ev.Bids) == 0 || len(ev.Asks) == 0 {
		return
	}
	bp, _ := strconv.ParseFloat(ev.Bids[0].Price, 64)
	ap, _ := strconv.ParseFloat(ev.Asks[0].Price, 64)
	mid := (bp + ap) / 2
	ask5 := sumNotionalAsks(ev.Asks, topDepthLevels)
	bid5 := sumNotionalBids(ev.Bids, topDepthLevels)
	now := time.Now()

	w.mu.Lock()
	defer w.mu.Unlock()

	w.lastPrice = mid
	w.Prices.Set(w.Symbol, mid)

	w.wallSnaps = append(w.wallSnaps, wallSnap{t: now, askUSDT: ask5, bidUSDT: bid5, mid: mid})
	cut := now.Add(-wallWindow)
	i := 0
	for i < len(w.wallSnaps) && w.wallSnaps[i].t.Before(cut) {
		i++
	}
	if i > 0 {
		w.wallSnaps = w.wallSnaps[i:]
	}
}

func sumNotionalBids(levels []futures.Bid, n int) float64 {
	var s float64
	for i := 0; i < n && i < len(levels); i++ {
		p, _ := strconv.ParseFloat(levels[i].Price, 64)
		q, _ := strconv.ParseFloat(levels[i].Quantity, 64)
		s += p * q
	}
	return s
}

func sumNotionalAsks(levels []futures.Ask, n int) float64 {
	var s float64
	for i := 0; i < n && i < len(levels); i++ {
		p, _ := strconv.ParseFloat(levels[i].Price, 64)
		q, _ := strconv.ParseFloat(levels[i].Quantity, 64)
		s += p * q
	}
	return s
}

func (w *SymbolWorker) trimFlow(legs *[]flowLeg, cut int64) {
	i := 0
	for i < len(*legs) && (*legs)[i].ms < cut {
		i++
	}
	if i > 0 {
		*legs = (*legs)[i:]
	}
}

func (w *SymbolWorker) trimMs(cut int64, ts *[]int64) {
	i := 0
	for i < len(*ts) && (*ts)[i] < cut {
		i++
	}
	if i > 0 {
		*ts = (*ts)[i:]
	}
}

func (w *SymbolWorker) trimPts(cut int64) {
	i := 0
	for i < len(w.pricePts) && w.pricePts[i].ms < cut {
		i++
	}
	if i > 0 {
		w.pricePts = w.pricePts[i:]
	}
}

func (w *SymbolWorker) tpsLocked(nowMs int64) int {
	cut := nowMs - 1000
	n := 0
	for i := len(w.tradeMs) - 1; i >= 0; i-- {
		if w.tradeMs[i] < cut {
			break
		}
		n++
	}
	return n
}

func (w *SymbolWorker) move10sLocked(nowMs int64) float64 {
	var first float64
	var firstSet bool
	for _, p := range w.pricePts {
		if p.ms >= nowMs-10_000 {
			if !firstSet {
				first = p.price
				firstSet = true
			}
		}
	}
	if !firstSet || w.lastPrice <= 0 || first <= 0 {
		return 0
	}
	return (w.lastPrice - first) / first
}

func (w *SymbolWorker) price2sAgo(nowMs int64) float64 {
	cut := nowMs - 2000
	for i := 0; i < len(w.pricePts); i++ {
		if w.pricePts[i].ms >= cut {
			return w.pricePts[i].price
		}
	}
	return 0
}

func (w *SymbolWorker) tickStrategy(hotOut *atomic.Bool) {
	now := time.Now()
	nowMs := now.UnixMilli()

	w.mu.Lock()
	// TPS окно
	w.trimMs(nowMs-3000, &w.tradeMs)
	w.trimPts(nowMs - 15_000)

	tps := w.tpsLocked(nowMs)
	move10 := w.move10sLocked(nowMs)
	hot := tps >= minTPS && math.Abs(move10) >= move10sMin
	if hot {
		w.lastHot = now
	}
	activeWatch := now.Sub(w.lastHot) <= staleOffWatch
	hotOut.Store(activeWatch)

	// 5s свеча close = lastPrice
	if now.Sub(w.lastBarTime) >= bbInterval && w.lastPrice > 0 {
		w.bbCloses = append(w.bbCloses, w.lastPrice)
		if len(w.bbCloses) > bbPeriod+5 {
			w.bbCloses = w.bbCloses[len(w.bbCloses)-(bbPeriod+5):]
		}
		w.lastBarTime = now
	}

	var buyV, sellV float64
	for _, l := range w.buyLegs {
		buyV += l.q
	}
	for _, l := range w.sellLegs {
		sellV += l.q
	}

	px := w.lastPrice
	p2 := w.price2sAgo(nowMs)
	snaps := append([]wallSnap(nil), w.wallSnaps...)
	closes := append([]float64(nil), w.bbCloses...)
	w.mu.Unlock()

	if !activeWatch || px <= 0 {
		return
	}

	if time.Now().UnixNano()-w.lastEntryNano.Load() < entryDebounce.Nanoseconds() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// Trigger 1: delta + 0.05% за 2с
	if sellV > 1 && buyV > 3.0*sellV && p2 > 0 && px >= p2*(1+priceUp2s) {
		w.lastEntryNano.Store(time.Now().UnixNano())
		_ = MarketOpenLong(ctx, w.Client, w.Symbol, w.Qty)
		w.Stats.PushImpulse(w.Symbol + " T1_DELTA_LONG")
		return
	}
	if buyV > 1 && sellV > 3.0*buyV && p2 > 0 && px <= p2*(1-priceUp2s) {
		w.lastEntryNano.Store(time.Now().UnixNano())
		_ = MarketOpenShort(ctx, w.Client, w.Symbol, w.Qty)
		w.Stats.PushImpulse(w.Symbol + " T1_DELTA_SHORT")
		return
	}

	// Trigger 2: стена 100k съедена за 500ms
	if len(snaps) >= 2 {
		old := snaps[0]
		cur := snaps[len(snaps)-1]
		if old.askUSDT-cur.askUSDT >= wallUSDT && cur.mid > old.mid {
			w.lastEntryNano.Store(time.Now().UnixNano())
			_ = MarketOpenLong(ctx, w.Client, w.Symbol, w.Qty)
			w.Stats.PushImpulse(w.Symbol + " T2_WALL_BUY")
			return
		}
		if old.bidUSDT-cur.bidUSDT >= wallUSDT && cur.mid < old.mid {
			w.lastEntryNano.Store(time.Now().UnixNano())
			_ = MarketOpenShort(ctx, w.Client, w.Symbol, w.Qty)
			w.Stats.PushImpulse(w.Symbol + " T2_WALL_SELL")
			return
		}
	}

	// Trigger 3: Bollinger 10 на 5s, пробой + высокий TPS
	if len(closes) >= bbPeriod && tps >= highTPSBreakout {
		_, up, low, ok := bollinger(closes, bbPeriod)
		if ok {
			lastC := closes[len(closes)-1]
			if lastC > up {
				w.lastEntryNano.Store(time.Now().UnixNano())
				_ = MarketOpenLong(ctx, w.Client, w.Symbol, w.Qty)
				w.Stats.PushImpulse(w.Symbol + " T3_BB_UP")
				return
			}
			if lastC < low {
				w.lastEntryNano.Store(time.Now().UnixNano())
				_ = MarketOpenShort(ctx, w.Client, w.Symbol, w.Qty)
				w.Stats.PushImpulse(w.Symbol + " T3_BB_DN")
				return
			}
		}
	}
}

func bollinger(closes []float64, period int) (sma, upper, lower float64, ok bool) {
	if len(closes) < period {
		return 0, 0, 0, false
	}
	slice := closes[len(closes)-period:]
	var sum float64
	for _, c := range slice {
		sum += c
	}
	sma = sum / float64(period)
	var v float64
	for _, c := range slice {
		d := c - sma
		v += d * d
	}
	v /= float64(period)
	sd := math.Sqrt(v)
	return sma, sma + 2*sd, sma - 2*sd, true
}
