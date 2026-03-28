package scanner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"binance-scalper/internal/strategy"

	"github.com/gorilla/websocket"
)

func futuresStreamBase() string {
	if u := strings.TrimSpace(os.Getenv("BINANCE_FSTREAM_WS")); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "wss://fstream.binance.com"
}

// Hub aggregates websocket-derived state for one symbol.
type Hub struct {
	symbolLower string
	mu          sync.RWMutex

	imbalance float64
	lastDepth time.Time

	closes   []float64
	maxClose int
	lastRSI  float64

	volBuckets []volBucket // last ~65s of quote volume (USDT)
	volSpike   bool

	// order flow: large trades (USDT)
	buyHits  []int64
	sellHits []int64

	markPrice float64

	onBuyBurst  func()
	onSellBurst func()
}

func NewHub(symbol string, maxClose int, onBuyBurst, onSellBurst func()) *Hub {
	s := strings.ToLower(symbol)
	if maxClose < 20 {
		maxClose = 20
	}
	return &Hub{
		symbolLower: s,
		maxClose:    maxClose,
		onBuyBurst:  onBuyBurst,
		onSellBurst: onSellBurst,
	}
}

func (h *Hub) SymbolLower() string { return h.symbolLower }

func (h *Hub) Imbalance() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.imbalance
}

func (h *Hub) RSI() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.lastRSI
}

func (h *Hub) VolumeSpike() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.volSpike
}

func (h *Hub) MarkPrice() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.markPrice
}

// SeedCloses sets historical closes (oldest first) for RSI warm-up.
func (h *Hub) SeedCloses(closes []float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closes = append([]float64(nil), closes...)
	if len(h.closes) > h.maxClose {
		h.closes = h.closes[len(h.closes)-h.maxClose:]
	}
	h.recomputeRSI()
}

func (h *Hub) updateImbalance(bids, asks [][]string) {
	var bidSum, askSum float64
	for i := 0; i < len(bids) && i < 10; i++ {
		if len(bids[i]) < 2 {
			continue
		}
		var q float64
		fmt.Sscanf(bids[i][1], "%f", &q)
		bidSum += q
	}
	for i := 0; i < len(asks) && i < 10; i++ {
		if len(asks[i]) < 2 {
			continue
		}
		var q float64
		fmt.Sscanf(asks[i][1], "%f", &q)
		askSum += q
	}
	den := bidSum + askSum
	if den <= 0 {
		h.imbalance = 0
		return
	}
	h.imbalance = (bidSum - askSum) / den
	h.lastDepth = time.Now()
}

func (h *Hub) addVolumeQuote(quoteVol float64, now time.Time) {
	sec := now.Unix()
	if n := len(h.volBuckets); n > 0 && h.volBuckets[n-1].sec == sec {
		h.volBuckets[n-1].vol += quoteVol
	} else {
		h.volBuckets = append(h.volBuckets, volBucket{sec: sec, vol: quoteVol})
	}
	cut := sec - 65
	i := 0
	for i < len(h.volBuckets) && h.volBuckets[i].sec < cut {
		i++
	}
	if i > 0 {
		h.volBuckets = append([]volBucket(nil), h.volBuckets[i:]...)
	}

	var sum60, sum10 float64
	for _, b := range h.volBuckets {
		if b.sec >= sec-60 {
			sum60 += b.vol
		}
		if b.sec >= sec-10 {
			sum10 += b.vol
		}
	}
	avgPer10 := sum60 / 6.0 // равномерная ожидаемая доля за 10с за минуту
	if avgPer10 <= 0 {
		h.volSpike = false
		return
	}
	h.volSpike = sum10 >= 3.0*avgPer10
}

func (h *Hub) handleAggTrade(price, qty float64, buyerMaker bool, eventMs int64) {
	quote := price * qty
	now := time.UnixMilli(eventMs)

	h.addVolumeQuote(quote, now)

	const minLarge = 1000.0
	const burstWindowMs = 500
	const need = 5

	if quote < minLarge {
		return
	}
	// Buyer is taker => aggressive buy
	if !buyerMaker {
		h.buyHits = append(h.buyHits, eventMs)
		h.buyHits = trimOlder(h.buyHits, eventMs-burstWindowMs)
		if len(h.buyHits) >= need && h.onBuyBurst != nil {
			h.onBuyBurst()
			h.buyHits = h.buyHits[:0]
		}
	} else {
		// Seller is taker => aggressive sell
		h.sellHits = append(h.sellHits, eventMs)
		h.sellHits = trimOlder(h.sellHits, eventMs-burstWindowMs)
		if len(h.sellHits) >= need && h.onSellBurst != nil {
			h.onSellBurst()
			h.sellHits = h.sellHits[:0]
		}
	}
}

func trimOlder(ts []int64, minTs int64) []int64 {
	i := 0
	for i < len(ts) && ts[i] < minTs {
		i++
	}
	return ts[i:]
}

func (h *Hub) pushClose(c float64) {
	if len(h.closes) >= h.maxClose {
		h.closes = h.closes[1:]
	}
	h.closes = append(h.closes, c)
}

func (h *Hub) recomputeRSI() {
	h.lastRSI = strategy.RSI(h.closes, 14)
}

type volBucket struct {
	sec int64
	vol float64
}

// RunCombined connects to combined stream and dispatches until error or ctx cancel.
func RunCombined(ctx context.Context, symbol string, hub *Hub) error {
	s := strings.ToLower(symbol)
	streams := strings.Join([]string{
		s + "@depth10@100ms",
		s + "@aggTrade",
		s + "@kline_1m",
		s + "@markPrice@1s",
	}, "/")
	u := futuresStreamBase() + "/stream?streams=" + streams

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.Dial(u, http.Header{})
	if err != nil {
		return err
	}
	defer conn.Close()

	go func() {
		<-ctx.Done()
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		_ = conn.Close()
	}()

	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	})

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(20 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
			case <-done:
				return
			}
		}
	}()
	defer close(done)

	for {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		var wrap map[string]json.RawMessage
		if err := json.Unmarshal(msg, &wrap); err != nil {
			continue
		}
		stream, ok := wrap["stream"]
		if !ok {
			continue
		}
		var streamStr string
		_ = json.Unmarshal(stream, &streamStr)

		data, ok := wrap["data"]
		if !ok {
			continue
		}

		if strings.Contains(streamStr, "@depth10") {
			var d struct {
				Bids [][]string `json:"b"`
				Asks [][]string `json:"a"`
			}
			if json.Unmarshal(data, &d) == nil {
				hub.mu.Lock()
				hub.updateImbalance(d.Bids, d.Asks)
				hub.mu.Unlock()
			}
			continue
		}
		if strings.Contains(streamStr, "@aggTrade") {
			var a struct {
				P string `json:"p"`
				Q string `json:"q"`
				M bool   `json:"m"`
				E int64  `json:"E"`
			}
			if json.Unmarshal(data, &a) == nil {
				var p, q float64
				fmt.Sscanf(a.P, "%f", &p)
				fmt.Sscanf(a.Q, "%f", &q)
				hub.mu.Lock()
				hub.handleAggTrade(p, q, a.M, a.E)
				hub.mu.Unlock()
			}
			continue
		}
		if strings.Contains(streamStr, "@kline_1m") {
			var k struct {
				K struct {
					C string `json:"c"`
					X bool   `json:"x"`
				} `json:"k"`
			}
			if json.Unmarshal(data, &k) == nil && k.K.X {
				var c float64
				fmt.Sscanf(k.K.C, "%f", &c)
				hub.mu.Lock()
				hub.pushClose(c)
				hub.recomputeRSI()
				hub.mu.Unlock()
			}
			continue
		}
		if strings.Contains(streamStr, "@markPrice") {
			var m struct {
				P string `json:"p"`
			}
			if json.Unmarshal(data, &m) == nil {
				var mp float64
				fmt.Sscanf(m.P, "%f", &mp)
				hub.mu.Lock()
				hub.markPrice = mp
				hub.mu.Unlock()
			}
		}
	}
}

func RunUserData(ctx context.Context, listenKey string, onOrder func(raw json.RawMessage)) error {
	u := futuresStreamBase() + "/ws/" + listenKey
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.Dial(u, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	go func() {
		<-ctx.Done()
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		_ = conn.Close()
	}()

	for {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		var ev struct {
			E string          `json:"e"`
			O json.RawMessage `json:"o"`
		}
		if json.Unmarshal(msg, &ev) != nil {
			continue
		}
		if ev.E == "ORDER_TRADE_UPDATE" && onOrder != nil {
			onOrder(msg)
		}
	}
}
