package hft

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/shopspring/decimal"
)

// Правила контракта из exchangeInfo (шаг кол-ва, min notional, тик цены).
type SymRules struct {
	QtyStep     decimal.Decimal
	MinQty      decimal.Decimal
	MaxQty      decimal.Decimal
	MinNotional decimal.Decimal
	PriceTick   decimal.Decimal
}

var (
	rulesMu             sync.RWMutex
	rulesDB             map[string]*SymRules
	notionalFloorUSDT   decimal.Decimal // демо часто требует ≥100 USDT нотионал
)

func refreshNotionalFloorForOrders() {
	notionalFloorUSDT = decimal.Zero
	base := strings.ToLower(futures.BaseApiMainUrl)
	if strings.Contains(base, "demo") {
		notionalFloorUSDT = decimal.NewFromInt(100)
	}
	if v := strings.TrimSpace(os.Getenv("MIN_NOTIONAL_FLOOR")); v != "" {
		if d, err := decimal.NewFromString(v); err == nil && d.GreaterThan(notionalFloorUSDT) {
			notionalFloorUSDT = d
		}
	}
}

// LoadFuturesRules загружает фильтры всех символов (один раз при старте).
func LoadFuturesRules(ctx context.Context, c *futures.Client) error {
	info, err := c.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return err
	}
	next := make(map[string]*SymRules)
	for _, s := range info.Symbols {
		r := parseFilters(s.Filters)
		if r != nil {
			next[s.Symbol] = r
		}
	}
	rulesMu.Lock()
	rulesDB = next
	rulesMu.Unlock()
	return nil
}

func parseFilters(filters []map[string]interface{}) *SymRules {
	r := &SymRules{
		QtyStep:     decimal.Zero,
		MinQty:      decimal.Zero,
		MaxQty:      decimal.RequireFromString("1e30"),
		MinNotional: decimal.Zero,
		PriceTick:   decimal.NewFromInt(1),
	}
	var lotStep, lotMin, lotMax string
	var mktStep, mktMin, mktMax string
	var tickSize string
	for _, f := range filters {
		ft, _ := f["filterType"].(string)
		switch ft {
		case "LOT_SIZE":
			lotStep, _ = f["stepSize"].(string)
			lotMin, _ = f["minQty"].(string)
			lotMax, _ = f["maxQty"].(string)
		case "MARKET_LOT_SIZE":
			mktStep, _ = f["stepSize"].(string)
			mktMin, _ = f["minQty"].(string)
			mktMax, _ = f["maxQty"].(string)
		case "MIN_NOTIONAL":
			if n, ok := f["notional"].(string); ok && n != "" {
				if d, err := decimal.NewFromString(n); err == nil {
					if d.GreaterThan(r.MinNotional) {
						r.MinNotional = d
					}
				}
			}
		case "PRICE_FILTER":
			tickSize, _ = f["tickSize"].(string)
		}
	}
	if mktStep != "" {
		r.QtyStep = mustDec(mktStep)
		r.MinQty = mustDec(mktMin)
		r.MaxQty = mustDec(mktMax)
	} else if lotStep != "" {
		r.QtyStep = mustDec(lotStep)
		r.MinQty = mustDec(lotMin)
		r.MaxQty = mustDec(lotMax)
	}
	if r.QtyStep.IsZero() {
		return nil
	}
	if tickSize != "" {
		r.PriceTick = mustDec(tickSize)
	}
	return r
}

func mustDec(s string) decimal.Decimal {
	d, err := decimal.NewFromString(strings.TrimSpace(s))
	if err != nil {
		return decimal.Zero
	}
	return d
}

func getRules(symbol string) *SymRules {
	rulesMu.RLock()
	defer rulesMu.RUnlock()
	if rulesDB == nil {
		return nil
	}
	return rulesDB[symbol]
}

// AdjustMarketQty подбирает quantity для MARKET: шаг LOT/MARKET_LOT, min/max qty, min notional (в USDT).
func AdjustMarketQty(ctx context.Context, c *futures.Client, symbol, qtyHint string) (string, error) {
	r := getRules(symbol)
	if r == nil {
		return strings.TrimSpace(qtyHint), nil
	}
	q, err := decimal.NewFromString(strings.TrimSpace(qtyHint))
	if err != nil {
		return "", err
	}
	mid, err := midPriceUSDT(ctx, c, symbol)
	if err != nil || mid <= 0 {
		return "", fmt.Errorf("цена для notional: %w", err)
	}
	price := decimal.NewFromFloat(mid)

	q = floorToStep(q, r.QtyStep)
	if q.LessThan(r.MinQty) {
		q = r.MinQty
	}
	effMin := r.MinNotional
	if notionalFloorUSDT.GreaterThan(effMin) {
		effMin = notionalFloorUSDT
	}
	if !effMin.IsZero() {
		n := q.Mul(price)
		if n.LessThan(effMin) {
			need := effMin.Div(price)
			q = ceilToStep(need, r.QtyStep)
		}
	}
	if q.GreaterThan(r.MaxQty) {
		q = r.MaxQty
	}
	if q.LessThan(r.MinQty) {
		return "", fmt.Errorf("%s: после min notional qty %s < minQty %s", symbol, q.String(), r.MinQty.String())
	}
	return trimQtyString(q, r.QtyStep), nil
}

func midPriceUSDT(ctx context.Context, c *futures.Client, symbol string) (float64, error) {
	tickers, err := c.NewListBookTickersService().Symbol(symbol).Do(ctx)
	if err != nil || len(tickers) == 0 {
		return 0, err
	}
	bp, _ := strconv.ParseFloat(tickers[0].BidPrice, 64)
	ap, _ := strconv.ParseFloat(tickers[0].AskPrice, 64)
	if bp <= 0 || ap <= 0 {
		return 0, fmt.Errorf("пустой book %s", symbol)
	}
	return (bp + ap) / 2, nil
}

func floorToStep(q, step decimal.Decimal) decimal.Decimal {
	if step.IsZero() {
		return q
	}
	n := q.Div(step).Floor()
	return n.Mul(step)
}

func ceilToStep(q, step decimal.Decimal) decimal.Decimal {
	if step.IsZero() {
		return q
	}
	n := q.Div(step).Ceil()
	return n.Mul(step)
}

func trimQtyString(q decimal.Decimal, step decimal.Decimal) string {
	stepStr := step.String()
	decimals := 0
	if i := strings.IndexByte(stepStr, '.'); i >= 0 {
		decimals = len(stepStr) - i - 1
	}
	out := q.StringFixed(int32(decimals))
	out = strings.TrimRight(strings.TrimRight(out, "0"), ".")
	if out == "" {
		out = "0"
	}
	return out
}

// FormatStopPrice округляет цену стопа вниз к tickSize (избегает -1111 по precision).
func FormatStopPrice(symbol string, price float64) string {
	r := getRules(symbol)
	if r == nil || r.PriceTick.IsZero() {
		return strconv.FormatFloat(price, 'f', 8, 64)
	}
	d := decimal.NewFromFloat(price)
	p := floorToStep(d, r.PriceTick)
	return trimQtyString(p, r.PriceTick)
}
