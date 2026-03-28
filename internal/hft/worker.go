package hft

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

// Пороги стратегии (переменные — чтобы ослабить на демо через ApplyDemoRelaxed).
var (
	MinTPS          = 30
	Move10sMin      = 0.001 // 0.1% за 10с
	StaleOffWatch   = 45 * time.Second
	BuySellRatio    = 3.0
	PriceUp2s       = 0.0005 // 0.05%
	WallUSDTThresh  = 100_000.0
	WallWindow      = 500 * time.Millisecond
	BBPeriod        = 10
	BBInterval      = 5 * time.Second
	EntryDebounce   = 900 * time.Millisecond
	TopDepthLevels  = 5
	HighTPSBreakout = 30
)

// ApplyDemoRelaxed снижает пороги (демо-счёт, больше сигналов для проверки).
func ApplyDemoRelaxed() {
	MinTPS = 12
	Move10sMin = 0.0004
	WallUSDTThresh = 20_000
	HighTPSBreakout = 12
	BuySellRatio = 2.2
}

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
		lastHot:     time.Time{}, // HOT только после реального «жара», не в момент старта
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
	if tps >= MinTPS && math.Abs(move10) >= Move10sMin {
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
	ask5 := sumNotionalAsks(ev.Asks, TopDepthLevels)
	bid5 := sumNotionalBids(ev.Bids, TopDepthLevels)
	now := time.Now()

	w.mu.Lock()
	defer w.mu.Unlock()

	w.lastPrice = mid
	w.Prices.Set(w.Symbol, mid)

	w.wallSnaps = append(w.wallSnaps, wallSnap{t: now, askUSDT: ask5, bidUSDT: bid5, mid: mid})
	cut := now.Add(-WallWindow)
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
	hot := tps >= MinTPS && math.Abs(move10) >= Move10sMin
	if hot {
		w.lastHot = now
	}
	// В watch только если есть цена и недавно был реальный горячий режим
	activeWatch := w.lastPrice > 0 && !w.lastHot.IsZero() && now.Sub(w.lastHot) <= StaleOffWatch
	hotOut.Store(activeWatch)

	// 5s свеча close = lastPrice
	if now.Sub(w.lastBarTime) >= BBInterval && w.lastPrice > 0 {
		w.bbCloses = append(w.bbCloses, w.lastPrice)
		if len(w.bbCloses) > BBPeriod+5 {
			w.bbCloses = w.bbCloses[len(w.bbCloses)-(BBPeriod+5):]
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

	if time.Now().UnixNano()-w.lastEntryNano.Load() < EntryDebounce.Nanoseconds() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// Trigger 1: delta + 0.05% за 2с
	if sellV > 1 && buyV > BuySellRatio*sellV && p2 > 0 && px >= p2*(1+PriceUp2s) {
		w.lastEntryNano.Store(time.Now().UnixNano())
		if err := MarketOpenLong(ctx, w.Client, w.Symbol, w.Qty); err != nil {
			w.Stats.SetOrderError(fmt.Sprintf("%s T1_LONG: %v", w.Symbol, err))
			w.Stats.PushImpulse(w.Symbol + " ERR " + err.Error())
			return
		}
		w.Stats.PushImpulse(w.Symbol + " T1_DELTA_LONG OK")
		return
	}
	if buyV > 1 && sellV > BuySellRatio*buyV && p2 > 0 && px <= p2*(1-PriceUp2s) {
		w.lastEntryNano.Store(time.Now().UnixNano())
		if err := MarketOpenShort(ctx, w.Client, w.Symbol, w.Qty); err != nil {
			w.Stats.SetOrderError(fmt.Sprintf("%s T1_SHORT: %v", w.Symbol, err))
			w.Stats.PushImpulse(w.Symbol + " ERR " + err.Error())
			return
		}
		w.Stats.PushImpulse(w.Symbol + " T1_DELTA_SHORT OK")
		return
	}

	// Trigger 2: стена съедена за ~500ms
	if len(snaps) >= 2 {
		old := snaps[0]
		cur := snaps[len(snaps)-1]
		if old.askUSDT-cur.askUSDT >= WallUSDTThresh && cur.mid > old.mid {
			w.lastEntryNano.Store(time.Now().UnixNano())
			if err := MarketOpenLong(ctx, w.Client, w.Symbol, w.Qty); err != nil {
				w.Stats.SetOrderError(fmt.Sprintf("%s T2_LONG: %v", w.Symbol, err))
				w.Stats.PushImpulse(w.Symbol + " ERR " + err.Error())
				return
			}
			w.Stats.PushImpulse(w.Symbol + " T2_WALL_BUY OK")
			return
		}
		if old.bidUSDT-cur.bidUSDT >= WallUSDTThresh && cur.mid < old.mid {
			w.lastEntryNano.Store(time.Now().UnixNano())
			if err := MarketOpenShort(ctx, w.Client, w.Symbol, w.Qty); err != nil {
				w.Stats.SetOrderError(fmt.Sprintf("%s T2_SHORT: %v", w.Symbol, err))
				w.Stats.PushImpulse(w.Symbol + " ERR " + err.Error())
				return
			}
			w.Stats.PushImpulse(w.Symbol + " T2_WALL_SELL OK")
			return
		}
	}

	// Trigger 3: Bollinger на 5s, пробой + высокий TPS
	if len(closes) >= BBPeriod && tps >= HighTPSBreakout {
		_, up, low, ok := bollinger(closes, BBPeriod)
		if ok {
			lastC := closes[len(closes)-1]
			if lastC > up {
				w.lastEntryNano.Store(time.Now().UnixNano())
				if err := MarketOpenLong(ctx, w.Client, w.Symbol, w.Qty); err != nil {
					w.Stats.SetOrderError(fmt.Sprintf("%s T3_LONG: %v", w.Symbol, err))
					w.Stats.PushImpulse(w.Symbol + " ERR " + err.Error())
					return
				}
				w.Stats.PushImpulse(w.Symbol + " T3_BB_UP OK")
				return
			}
			if lastC < low {
				w.lastEntryNano.Store(time.Now().UnixNano())
				if err := MarketOpenShort(ctx, w.Client, w.Symbol, w.Qty); err != nil {
					w.Stats.SetOrderError(fmt.Sprintf("%s T3_SHORT: %v", w.Symbol, err))
					w.Stats.PushImpulse(w.Symbol + " ERR " + err.Error())
					return
				}
				w.Stats.PushImpulse(w.Symbol + " T3_BB_DN OK")
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
