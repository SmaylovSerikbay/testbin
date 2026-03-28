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
	"net"
	"net/http"
	"net/url"
	"sort"
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

	bracketMu   sync.Mutex
	bracketMaxL map[string]int // символ → max initialLeverage для нашего нотиoнала (кэш)
}

type lotSpec struct {
	step   float64
	minQ   float64
	minNtl float64 // MIN_NOTIONAL в USDT (новые правила ~5)
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

func fastFuturesTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   4 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   8 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
}

func newFAPIClient(baseREST, apiKey, apiSecret string) *fapiClient {
	return &fapiClient{
		base:   strings.TrimSuffix(baseREST, "/"),
		key:    apiKey,
		secret: apiSecret,
		httpc: &http.Client{
			Transport: fastFuturesTransport(),
			Timeout:   25 * time.Second,
		},
		lots:        make(map[string]lotSpec),
		levOK:       make(map[string]int),
		bracketMaxL: make(map[string]int),
	}
}

// WarmupPing — прогрев TLS/TCP и пула к FAPI (меньше задержка на первом ордере).
func (c *fapiClient) WarmupPing(ctx context.Context) (time.Duration, error) {
	t0 := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/fapi/v1/ping", nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 256))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return time.Since(t0), fmt.Errorf("ping HTTP %d", resp.StatusCode)
	}
	return time.Since(t0), nil
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
				FilterType  string `json:"filterType"`
				MinQty      string `json:"minQty"`
				StepSize    string `json:"stepSize"`
				Notional    string `json:"notional"`
				MinNotional string `json:"minNotional"`
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
		// Несколько фильтров (legacy MIN_NOTIONAL + NOTIONAL) — берём максимум, иначе можно занизить и словить −4164.
		minNtl := 5.0
		for _, f := range s.Filters {
			if f.FilterType != "MIN_NOTIONAL" && f.FilterType != "NOTIONAL" {
				continue
			}
			for _, raw := range []string{f.Notional, f.MinNotional} {
				if raw == "" {
					continue
				}
				if v, e := strconv.ParseFloat(raw, 64); e == nil && v > minNtl {
					minNtl = v
				}
			}
		}
		tmp[s.Symbol] = lotSpec{step: step, minQ: minQ, minNtl: minNtl}
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

// PremiumMarkIndex — mark и index; для min-notional берём min(·): биржа может опираться на более низкий из них → меньше −4164.
func (c *fapiClient) PremiumMarkIndex(ctx context.Context, symbol string) (mark, index float64, err error) {
	u := c.base + "/fapi/v1/premiumIndex?symbol=" + url.QueryEscape(symbol)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, 0, err
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 256<<10))
	if err != nil {
		return 0, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		sn := len(raw)
		if sn > 220 {
			sn = 220
		}
		return 0, 0, fmt.Errorf("premiumIndex HTTP %d: %s", resp.StatusCode, string(raw[:sn]))
	}
	var row struct {
		MarkPrice  string `json:"markPrice"`
		IndexPrice string `json:"indexPrice"`
	}
	if err := json.Unmarshal(raw, &row); err != nil {
		return 0, 0, err
	}
	if row.MarkPrice == "" {
		return 0, 0, fmt.Errorf("premiumIndex: пустой markPrice")
	}
	mark, err = strconv.ParseFloat(row.MarkPrice, 64)
	if err != nil || mark <= 0 {
		return 0, 0, fmt.Errorf("premiumIndex: markPrice %q", row.MarkPrice)
	}
	if row.IndexPrice != "" {
		index, _ = strconv.ParseFloat(row.IndexPrice, 64)
	}
	if index <= 0 {
		index = mark
	}
	return mark, index, nil
}

// MarkPrice — публичный mark (без ключа).
func (c *fapiClient) MarkPrice(ctx context.Context, symbol string) (float64, error) {
	m, _, err := c.PremiumMarkIndex(ctx, symbol)
	return m, err
}

func roundQtyDown(q, step float64) float64 {
	if step <= 0 {
		return q
	}
	return math.Floor(q/step+1e-12) * step
}

func roundQtyCeil(q, step float64) float64 {
	if step <= 0 {
		return q
	}
	return math.Ceil(q/step-1e-12) * step
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

// BookTickerBid — публичный best bid (ниже last → больше qty при том же нотиoнале, меньше −4164).
func (c *fapiClient) BookTickerBid(ctx context.Context, symbol string) (float64, error) {
	u := c.base + "/fapi/v1/ticker/bookTicker?symbol=" + url.QueryEscape(symbol)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 128<<10))
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		sn := len(raw)
		if sn > 200 {
			sn = 200
		}
		return 0, fmt.Errorf("bookTicker HTTP %d: %s", resp.StatusCode, string(raw[:sn]))
	}
	var row struct {
		BidPrice string `json:"bidPrice"`
	}
	if err := json.Unmarshal(raw, &row); err != nil {
		return 0, err
	}
	if row.BidPrice == "" {
		return 0, fmt.Errorf("bookTicker: пустой bid")
	}
	b, err := strconv.ParseFloat(row.BidPrice, 64)
	if err != nil || b <= 0 {
		return 0, fmt.Errorf("bookTicker: bid %q", row.BidPrice)
	}
	return b, nil
}

// notionalCheckRef — консервативная цена для min-notional (лонг): ниже цена → меньше USDT на тот же qty.
func notionalCheckRef(mark, index float64) float64 {
	if mark <= 0 {
		return index
	}
	if index <= 0 {
		return mark
	}
	return math.Min(mark, index)
}

// notionalBudgetRef — верхняя оценка USDT на 1 контракт для лимита нотиoнала: max(mark,index), иначе qty по index проходит min-notional, а qty×mark ломает бюджет.
func notionalBudgetRef(mark, index float64) float64 {
	if mark <= 0 {
		return index
	}
	if index <= 0 {
		return mark
	}
	if mark > index {
		return mark
	}
	return index
}

const minNotionalOrderSlack = 1.022 // запас к лимиту биржи и рассинхрону mark/index между REST-вызовами

// PrepareQtyBuyLive — qty из notional / max(сигнал, max(mark,index)) с небольшим запасом, затем дожим шагами
// пока qty×min(mark,index) ≥ MIN_NOTIONAL×slack и qty×max(mark,index) ≤ notional.
// Раньше первичный qty считали по min(ref,bid,min(m,i)) — получалось завышение qty и срыв по бюджету.
func (c *fapiClient) PrepareQtyBuyLive(ctx context.Context, symbol string, notionalUSDT, refSignal float64) (qtyStr string, qty float64, err error) {
	if notionalUSDT <= 0 || refSignal <= 0 {
		return "", 0, fmt.Errorf("некорректная цена/нотиoнал")
	}
	ls, err := c.lotFor(symbol)
	if err != nil {
		return "", 0, err
	}
	minN := ls.minNtl
	if minN <= 0 {
		minN = 5
	}
	minNeed := minN * minNotionalOrderSlack

	mp, idx, e := c.PremiumMarkIndex(ctx, symbol)
	if e != nil || mp <= 0 {
		mp, idx = refSignal, refSignal
	}
	budgetRef0 := notionalBudgetRef(mp, idx)
	refForQty := budgetRef0
	if refSignal > refForQty {
		refForQty = refSignal
	}
	const priceSizingSlack = 1.003 // проскальзывание / тик к mark
	refForQty *= priceSizingSlack
	if refForQty <= 0 {
		refForQty = refSignal
	}

	qtyStr, qty, err = c.PrepareQtyBuy(symbol, notionalUSDT, refForQty)
	if err != nil {
		return "", 0, err
	}
	for attempt := 0; attempt < 64; attempt++ {
		mp2, idx2, e2 := c.PremiumMarkIndex(ctx, symbol)
		if e2 != nil || mp2 <= 0 {
			mp2, idx2 = mp, idx
		}
		ntlRef := notionalCheckRef(mp2, idx2)
		budgetRef := notionalBudgetRef(mp2, idx2)
		if qty*budgetRef > notionalUSDT+1e-6 {
			qtyMinN := roundQtyCeil(minNeed/ntlRef, ls.step)
			if qtyMinN < ls.minQ {
				qtyMinN = ls.minQ
			}
			needNtl := qtyMinN * budgetRef
			return "", 0, fmt.Errorf("min notional %.2f: при min(mark,index)≈%.8f max≈%.8f нужно ≥%.2f USDT нотиoнала под шаг лота, задано %.2f (%s)",
				minN, ntlRef, budgetRef, needNtl, notionalUSDT, symbol)
		}
		if qty*ntlRef >= minNeed-1e-10 {
			out := formatQtyString(qty, ls.step)
			qp, perr := strconv.ParseFloat(out, 64)
			if perr != nil || qp <= 0 {
				return out, qty, nil
			}
			if qp*ntlRef >= minNeed-1e-10 {
				return out, qp, nil
			}
			qty = roundQtyCeil(qp+ls.step, ls.step)
			if qty < ls.minQ {
				qty = ls.minQ
			}
			continue
		}
		prev := qty
		qty = roundQtyCeil(prev+ls.step, ls.step)
		if qty <= prev {
			qty = prev + ls.step
		}
		if qty < ls.minQ {
			qty = ls.minQ
		}
		qtyStr = formatQtyString(qty, ls.step)
	}
	mp3, idx3, _ := c.PremiumMarkIndex(ctx, symbol)
	if mp3 <= 0 {
		mp3, idx3 = mp, idx
	}
	ntlRef := notionalCheckRef(mp3, idx3)
	return "", 0, fmt.Errorf("qty×min(mark,index)≈%.6f < min notional %.2f×slack после дожима (%s)", qty*ntlRef, minN, symbol)
}

// BumpQtyAfter4164 — после отказа биржи по min-notional: поднять qty по шагу, не превышая notionalCap по mark.
func (c *fapiClient) BumpQtyAfter4164(ctx context.Context, symbol, qtyStr string, notionalCap float64) (string, error) {
	ls, err := c.lotFor(symbol)
	if err != nil {
		return "", err
	}
	minN := ls.minNtl
	if minN <= 0 {
		minN = 5
	}
	minNeed := minN * minNotionalOrderSlack
	qty, err := strconv.ParseFloat(strings.TrimSpace(qtyStr), 64)
	if err != nil || qty <= 0 {
		return "", fmt.Errorf("qty из строки %q: %w", qtyStr, err)
	}
	for attempt := 0; attempt < 56; attempt++ {
		mp, idx, e := c.PremiumMarkIndex(ctx, symbol)
		if e != nil {
			return "", e
		}
		if mp <= 0 {
			return "", fmt.Errorf("premiumIndex: mark≤0 (%s)", symbol)
		}
		ntlRef := notionalCheckRef(mp, idx)
		budgetRef := notionalBudgetRef(mp, idx)
		if qty*budgetRef > notionalCap+1e-5 {
			return "", fmt.Errorf("−4164 retry: qty×max(mark,index)≈%.6f > лимит %.2f (%s)", qty*budgetRef, notionalCap, symbol)
		}
		if qty*ntlRef >= minNeed-1e-10 {
			return formatQtyString(qty, ls.step), nil
		}
		next := roundQtyCeil(qty+ls.step, ls.step)
		if next <= qty {
			next = qty + ls.step
		}
		if next < ls.minQ {
			next = ls.minQ
		}
		qty = next
	}
	return "", fmt.Errorf("−4164 retry: не удалось добить min-notional (%s)", symbol)
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

func (c *fapiClient) getSigned(ctx context.Context, path string, params url.Values) ([]byte, error) {
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("recvWindow", "5000")
	query := params.Encode()
	mac := hmac.New(sha256.New, []byte(c.secret))
	mac.Write([]byte(query))
	sig := hex.EncodeToString(mac.Sum(nil))
	full := c.base + path + "?" + query + "&signature=" + sig
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
	if err != nil {
		return nil, err
	}
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

// FuturesUSDTBalance — кошелёк USDT-M (кросс): кошелёк и доступно для новых ордеров.
func (c *fapiClient) FuturesUSDTBalance(ctx context.Context) (wallet, available float64, err error) {
	raw, err := c.getSigned(ctx, "/fapi/v2/account", url.Values{})
	if err != nil {
		return 0, 0, err
	}
	var acc struct {
		TotalWalletBalance string `json:"totalWalletBalance"`
		AvailableBalance   string `json:"availableBalance"`
		Code               int    `json:"code"`
		Msg                string `json:"msg"`
	}
	if err := json.Unmarshal(raw, &acc); err != nil {
		return 0, 0, err
	}
	if acc.Code != 0 {
		return 0, 0, fmt.Errorf("account %d: %s", acc.Code, acc.Msg)
	}
	wallet, _ = strconv.ParseFloat(acc.TotalWalletBalance, 64)
	available, _ = strconv.ParseFloat(acc.AvailableBalance, 64)
	return wallet, available, nil
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

func orderRespFromRaw(raw []byte) (fapiOrderResp, error) {
	var o fapiOrderResp
	if err := json.Unmarshal(raw, &o); err != nil {
		return o, err
	}
	if o.Code != 0 {
		return o, fmt.Errorf("binance %d: %s", o.Code, o.Msg)
	}
	return o, nil
}

func (c *fapiClient) pollOrderUntilFill(ctx context.Context, symbol string, orderID int64) ([]byte, error) {
	interval := 150 * time.Millisecond
	for attempt := 0; attempt < 80; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(interval):
			}
		}
		p := url.Values{}
		p.Set("symbol", symbol)
		p.Set("orderId", strconv.FormatInt(orderID, 10))
		raw, err := c.getSigned(ctx, "/fapi/v1/order", p)
		if err != nil {
			continue
		}
		o, err := orderRespFromRaw(raw)
		if err != nil {
			continue
		}
		switch strings.ToUpper(o.Status) {
		case "FILLED":
			return raw, nil
		case "CANCELED", "EXPIRED", "REJECTED":
			return raw, fmt.Errorf("ордер %d: статус %s", orderID, o.Status)
		}
	}
	return nil, fmt.Errorf("таймаут ожидания исполнения ордера %d", orderID)
}

func (c *fapiClient) parseFillOrPoll(ctx context.Context, symbol string, raw []byte) (avgPrice, qty float64, err error) {
	o, err := orderRespFromRaw(raw)
	if err != nil {
		return 0, 0, err
	}
	qty, _ = strconv.ParseFloat(o.ExecutedQty, 64)
	if qty > 0 {
		avg, _ := strconv.ParseFloat(o.AvgPrice, 64)
		if avg <= 0 {
			cq, _ := strconv.ParseFloat(o.CumQuote, 64)
			if cq > 0 {
				avg = cq / qty
			}
		}
		return avg, qty, nil
	}
	if o.OrderID > 0 && (o.Status == "NEW" || o.Status == "PARTIALLY_FILLED" || o.Status == "") {
		raw2, err := c.pollOrderUntilFill(ctx, symbol, o.OrderID)
		if err != nil {
			return 0, 0, err
		}
		return parseOrderFill(raw2)
	}
	return 0, 0, fmt.Errorf("пустой executedQty, status=%s", o.Status)
}

func parseFloatJSON(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

// maxLeverageForNotional — по leverageBracket: допустимое initialLeverage для объёма позиции (USDT).
func (c *fapiClient) maxLeverageForNotional(ctx context.Context, symbol string, notionalUSDT float64) (int, error) {
	c.bracketMu.Lock()
	if m, ok := c.bracketMaxL[symbol]; ok {
		c.bracketMu.Unlock()
		return m, nil
	}
	c.bracketMu.Unlock()

	p := url.Values{}
	p.Set("symbol", symbol)
	raw, err := c.getSigned(ctx, "/fapi/v1/leverageBracket", p)
	if err != nil {
		return 0, err
	}
	var rows []struct {
		Symbol   string `json:"symbol"`
		Brackets []struct {
			InitialLeverage int `json:"initialLeverage"`
			NotionalCap     any `json:"notionalCap"`
			NotionalFloor   any `json:"notionalFloor"`
		} `json:"brackets"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		return 0, err
	}
	var br []struct {
		InitialLeverage int
		NotionalCap     float64
		NotionalFloor   float64
	}
	for _, row := range rows {
		if row.Symbol != symbol {
			continue
		}
		for _, b := range row.Brackets {
			capF := anyToFloat(b.NotionalCap)
			flF := anyToFloat(b.NotionalFloor)
			br = append(br, struct {
				InitialLeverage int
				NotionalCap     float64
				NotionalFloor   float64
			}{b.InitialLeverage, capF, flF})
		}
		break
	}
	if len(br) == 0 {
		return 0, fmt.Errorf("нет brackets для %s", symbol)
	}
	sort.Slice(br, func(i, j int) bool { return br[i].NotionalFloor < br[j].NotionalFloor })
	n := notionalUSDT
	if n <= 0 {
		n = 1
	}
	maxLev := br[len(br)-1].InitialLeverage
	for _, b := range br {
		capOK := b.NotionalCap <= 0 || n < b.NotionalCap
		if n >= b.NotionalFloor && capOK {
			maxLev = b.InitialLeverage
			break
		}
	}
	c.bracketMu.Lock()
	c.bracketMaxL[symbol] = maxLev
	c.bracketMu.Unlock()
	return maxLev, nil
}

func anyToFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		return parseFloatJSON(x)
	default:
		b, _ := json.Marshal(x)
		return parseFloatJSON(strings.Trim(string(b), `"`))
	}
}

// ensureLeverage выставляет плечо с учётом leverageBracket; возвращает фактическое плечо (≤ wantLev).
func (c *fapiClient) ensureLeverage(ctx context.Context, symbol string, wantLev int, targetNotionalUSDT float64) (int, error) {
	if wantLev < 1 {
		wantLev = 1
	}
	if wantLev > 125 {
		wantLev = 125
	}
	maxL, err := c.maxLeverageForNotional(ctx, symbol, targetNotionalUSDT)
	if err != nil {
		maxL = wantLev
	}
	useLev := wantLev
	if maxL > 0 && useLev > maxL {
		useLev = maxL
	}
	if useLev < 1 {
		useLev = 1
	}
	c.levMu.Lock()
	if cur, ok := c.levOK[symbol]; ok && cur == useLev {
		c.levMu.Unlock()
		return useLev, nil
	}
	c.levMu.Unlock()

	p := url.Values{}
	p.Set("symbol", symbol)
	p.Set("leverage", strconv.Itoa(useLev))
	raw, err := c.postSigned(ctx, "/fapi/v1/leverage", p)
	if err != nil {
		return 0, err
	}
	var out struct {
		Leverage int    `json:"leverage"`
		Symbol   string `json:"symbol"`
		Code     int    `json:"code"`
		Msg      string `json:"msg"`
	}
	if json.Unmarshal(raw, &out) == nil && out.Code != 0 {
		return 0, fmt.Errorf("leverage %d: %s", out.Code, out.Msg)
	}
	c.levMu.Lock()
	c.levOK[symbol] = useLev
	c.levMu.Unlock()
	return useLev, nil
}

// MarketBuyLong — рыночный лонг по количеству (уже округлённому).
func (c *fapiClient) MarketBuyLong(ctx context.Context, symbol, qtyStr string, hedge bool) (avgPrice, qty float64, err error) {
	p := url.Values{}
	p.Set("symbol", symbol)
	p.Set("side", "BUY")
	p.Set("type", "MARKET")
	p.Set("quantity", qtyStr)
	p.Set("newOrderRespType", "RESULT")
	if hedge {
		p.Set("positionSide", "LONG")
	}
	raw, err := c.postSigned(ctx, "/fapi/v1/order", p)
	if err != nil {
		return 0, 0, err
	}
	return c.parseFillOrPoll(ctx, symbol, raw)
}

// MarketSellClose — рыночное закрытие лонга (reduceOnly).
func (c *fapiClient) MarketSellClose(ctx context.Context, symbol, qtyStr string, hedge bool) (avgPrice, qty float64, err error) {
	p := url.Values{}
	p.Set("symbol", symbol)
	p.Set("side", "SELL")
	p.Set("type", "MARKET")
	p.Set("quantity", qtyStr)
	p.Set("reduceOnly", "true")
	p.Set("newOrderRespType", "RESULT")
	if hedge {
		p.Set("positionSide", "LONG")
	}
	raw, err := c.postSigned(ctx, "/fapi/v1/order", p)
	if err != nil {
		return 0, 0, err
	}
	return c.parseFillOrPoll(ctx, symbol, raw)
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
	minN := ls.minNtl
	if minN <= 0 {
		minN = 5
	}
	need := roundQtyCeil(minN/refPrice, ls.step)
	if need < ls.minQ {
		need = ls.minQ
	}
	if qty < need {
		qty = need
	}
	if qty*refPrice > notionalUSDT+1e-8 {
		return "", 0, fmt.Errorf("min notional %.2f USDT требует ~%.4f монет при цене %.8f, бюджет %.2f USDT (%s)",
			minN, need, refPrice, notionalUSDT, symbol)
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
