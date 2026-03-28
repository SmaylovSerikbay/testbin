package hft

import "sync"

// PriceHub — последняя цена сделки по символу (для трейлинга / mark-proxy).
type PriceHub struct {
	mu sync.RWMutex
	m  map[string]float64
}

func NewPriceHub() *PriceHub {
	return &PriceHub{m: make(map[string]float64)}
}

func (h *PriceHub) Set(symbol string, px float64) {
	h.mu.Lock()
	h.m[symbol] = px
	h.mu.Unlock()
}

func (h *PriceHub) Get(symbol string) float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.m[symbol]
}
