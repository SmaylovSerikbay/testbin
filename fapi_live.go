package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// fapiClient — подписанные запросы к Binance USDT-M Futures (боевой или demo-fapi).
type fapiClient struct {
	base   string
	key    string
	secret string
	httpc  *http.Client

	lotMu sync.RWMutex
	lots  map[string]lotSpec

	levMu sync.Mutex
	levOK map[string]int // символ → выставленное плечо
}

type lotSpec struct {
	step float64
	minQ float64
}

type fapiOrderResp struct {
	AvgPrice     string `json:"avgPrice"`
	ExecutedQty  string `json:"executedQty"`
	CumQuote     string `json:"cumQuote"`
	Code         int    `json:"code"`
	Msg          string `json:"msg"`
	OrderID      int64  `json:"orderId"`
	ReduceOnly   bool   `json:"reduceOnly"`
	ClientOId    string `json:"clientOrderId"`
	Status       string `json:"status"`
	PositionSide string `json:"positionSide"`
}

func newFAPIClient(baseREST, apiKey, apiSecret string) *fapiClient {
	return &fapiClient{
		base:   strings.TrimSuffix(baseREST, "/"),
		key:    apiKey,
		secret: apiSecret,
		httpc:  &http.Client{Timeout: 25 * time.Second},
		lots:   make(map[string]lotSpec),
		levOK:  make(map[string]int),
	}
}

func (c *fapiClient) loadLotSpecs(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/fapi/v1/exchangeInfo", nil)
	if err != nil {
		return err
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		sn := len(b)
		if sn > 512 {
			sn = 512
		}
		return fmt.Errorf("exchangeInfo HTTP %d: %s", resp.StatusCode, string(b[:sn]))
	}
	var wrap struct {
		Symbols []struct {
			Symbol  string `json:"symbol"`
			Filters []struct {
				FilterType string `json:"filterType"`
				MinQty     string `json:"minQty"`
				StepSize   string `json:"stepSize"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(b, &wrap); err != nil {
		return err
	}
	tmp := make(map[string]lotSpec)
	for _, s := range wrap.Symbols {
		var stepStr, minStr string
		for _, f := range s.Filters {
			if f.FilterType == "MARKET_LOT_SIZE" {
				stepStr, minStr = f.StepSize, f.MinQty
				break
			}
		}
		if stepStr == "" {
			for _, f := range s.Filters {
				if f.FilterType == "LOT_SIZE" {
					stepStr, minStr = f.StepSize, f.MinQty
					break
				}
			}
		}
		if stepStr == "" {
			continue
		}
		step, e1 := strconv.ParseFloat(stepStr, 64)
		minQ, e2 := strconv.ParseFloat(minStr, 64)
		if e1 != nil || e2 != nil || step <= 0 || minQ < 0 {
			continue
		}
		tmp[s.Symbol] = lotSpec{step: step, minQ: minQ}
	}
	c.lotMu.Lock()
	c.lots = tmp
	c.lotMu.Unlock()
	return nil
}

func (c *fapiClient) lotFor(symbol string) (lotSpec, error) {
	c.lotMu.RLock()
	ls, ok := c.lots[symbol]
	c.lotMu.RUnlock()
	if !ok {
		return lotSpec{}, fmt.Errorf("нет LOT/MARKET_LOT для %s", symbol)
	}
	return ls, nil
}

func roundQtyDown(q, step float64) float64 {
	if step <= 0 {
		return q
	}
	return math.Floor(q/step+1e-12) * step
}

func formatQtyString(q float64, step float64) string {
	prec := 0
	stepStr := strconv.FormatFloat(step, 'f', -1, 64)
	if i := strings.IndexByte(stepStr, '.'); i >= 0 {
		prec = len(stepStr) - i - 1
		for prec > 0 && stepStr[len(stepStr)-1] == '0' {
			stepStr = stepStr[:len(stepStr)-1]
			prec--
		}
		if prec < 0 {
			prec = 0
		}
	}
	s := strconv.FormatFloat(q, 'f', prec, 64)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	if s == "" || s == "-" {
		s = "0"
	}
	return s
}

func (c *fapiClient) postSigned(ctx context.Context, path string, params url.Values) ([]byte, error) {
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("recvWindow", "5000")
	query := params.Encode()
	mac := hmac.New(sha256.New, []byte(c.secret))
	mac.Write([]byte(query))
	sig := hex.EncodeToString(mac.Sum(nil))
	body := query + "&signature=" + sig
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-MBX-APIKEY", c.key)
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(raw))
	}
	return raw, nil
}

func parseOrderFill(raw []byte) (avgPrice, qty float64, err error) {
	var o fapiOrderResp
	if err := json.Unmarshal(raw, &o); err != nil {
		return 0, 0, err
	}
	if o.Code != 0 {
		return 0, 0, fmt.Errorf("binance %d: %s", o.Code, o.Msg)
	}
	qty, _ = strconv.ParseFloat(o.ExecutedQty, 64)
	avg, _ := strconv.ParseFloat(o.AvgPrice, 64)
	if qty > 0 && avg <= 0 {
		cq, _ := strconv.ParseFloat(o.CumQuote, 64)
		if cq > 0 {
			avg = cq / qty
		}
	}
	if qty <= 0 {
		return 0, 0, fmt.Errorf("пустой executedQty, status=%s", o.Status)
	}
	return avg, qty, nil
}

func (c *fapiClient) ensureLeverage(ctx context.Context, symbol string, lev int) error {
	if lev < 1 {
		lev = 1
	}
	if lev > 125 {
		lev = 125
	}
	c.levMu.Lock()
	if cur, ok := c.levOK[symbol]; ok && cur == lev {
		c.levMu.Unlock()
		return nil
	}
	c.levMu.Unlock()

	p := url.Values{}
	p.Set("symbol", symbol)
	p.Set("leverage", strconv.Itoa(lev))
	raw, err := c.postSigned(ctx, "/fapi/v1/leverage", p)
	if err != nil {
		return err
	}
	var out struct {
		Leverage int    `json:"leverage"`
		Symbol   string `json:"symbol"`
		Code     int    `json:"code"`
		Msg      string `json:"msg"`
	}
	if json.Unmarshal(raw, &out) == nil && out.Code != 0 {
		return fmt.Errorf("leverage %d: %s", out.Code, out.Msg)
	}
	c.levMu.Lock()
	c.levOK[symbol] = lev
	c.levMu.Unlock()
	return nil
}

// MarketBuyLong — рыночный лонг по количеству (уже округлённому).
func (c *fapiClient) MarketBuyLong(ctx context.Context, symbol, qtyStr string, hedge bool) (avgPrice, qty float64, err error) {
	p := url.Values{}
	p.Set("symbol", symbol)
	p.Set("side", "BUY")
	p.Set("type", "MARKET")
	p.Set("quantity", qtyStr)
	if hedge {
		p.Set("positionSide", "LONG")
	}
	raw, err := c.postSigned(ctx, "/fapi/v1/order", p)
	if err != nil {
		return 0, 0, err
	}
	return parseOrderFill(raw)
}

// MarketSellClose — рыночное закрытие лонга (reduceOnly).
func (c *fapiClient) MarketSellClose(ctx context.Context, symbol, qtyStr string, hedge bool) (avgPrice, qty float64, err error) {
	p := url.Values{}
	p.Set("symbol", symbol)
	p.Set("side", "SELL")
	p.Set("type", "MARKET")
	p.Set("quantity", qtyStr)
	p.Set("reduceOnly", "true")
	if hedge {
		p.Set("positionSide", "LONG")
	}
	raw, err := c.postSigned(ctx, "/fapi/v1/order", p)
	if err != nil {
		return 0, 0, err
	}
	return parseOrderFill(raw)
}

// PrepareQtyBuy округляет количество к шагу лота и проверяет minQty.
func (c *fapiClient) PrepareQtyBuy(symbol string, notionalUSDT, refPrice float64) (qtyStr string, qty float64, err error) {
	if refPrice <= 0 || notionalUSDT <= 0 {
		return "", 0, fmt.Errorf("некорректная цена/нотиoнал")
	}
	ls, err := c.lotFor(symbol)
	if err != nil {
		return "", 0, err
	}
	raw := notionalUSDT / refPrice
	qty = roundQtyDown(raw, ls.step)
	if qty < ls.minQ {
		return "", 0, fmt.Errorf("qty %.8f < minQty %.8f для %s", qty, ls.minQ, symbol)
	}
	return formatQtyString(qty, ls.step), qty, nil
}

// PrepareQtyClose округляет закрытие к шагу (не больше открытого qty).
func (c *fapiClient) PrepareQtyClose(symbol string, openQty float64) (qtyStr string, qty float64, err error) {
	ls, err := c.lotFor(symbol)
	if err != nil {
		return "", 0, err
	}
	qty = roundQtyDown(openQty, ls.step)
	if qty < ls.minQ {
		return "", 0, fmt.Errorf("close qty %.8f < minQty %.8f", qty, ls.minQ)
	}
	return formatQtyString(qty, ls.step), qty, nil
}
