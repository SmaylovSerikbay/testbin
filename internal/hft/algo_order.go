package hft

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

func fapiSign(query, secret string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(query))
	return hex.EncodeToString(m.Sum(nil))
}

func fapiNowMs(c *futures.Client) int64 {
	return time.Now().UnixMilli() + int64(c.TimeOffset)
}

// fapiPostForm signed POST application/x-www-form-urlencoded.
func fapiPostForm(ctx context.Context, c *futures.Client, path string, v url.Values) ([]byte, error) {
	if v == nil {
		v = url.Values{}
	}
	v.Set("timestamp", strconv.FormatInt(fapiNowMs(c), 10))
	v.Set("recvWindow", "5000")
	qs := v.Encode()
	sig := fapiSign(qs, c.SecretKey)
	bodyStr := qs + "&signature=" + sig
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, strings.NewReader(bodyStr))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-MBX-APIKEY", c.APIKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, parseAPIErr(b)
	}
	if err := checkFapiBodyError(b); err != nil {
		return nil, err
	}
	return b, nil
}

func checkFapiBodyError(b []byte) error {
	var chk map[string]interface{}
	if json.Unmarshal(b, &chk) != nil {
		return nil
	}
	v, ok := chk["code"]
	if !ok {
		return nil
	}
	switch t := v.(type) {
	case float64:
		if t != 0 && t != 200 {
			msg, _ := chk["msg"].(string)
			return fmt.Errorf("<APIError> code=%v, msg=%s", t, msg)
		}
	case string:
		if t != "" && t != "200" {
			msg, _ := chk["msg"].(string)
			return fmt.Errorf("<APIError> code=%s, msg=%s", t, msg)
		}
	}
	return nil
}

func fapiGetSigned(ctx context.Context, c *futures.Client, path string, v url.Values) ([]byte, error) {
	if v == nil {
		v = url.Values{}
	}
	v.Set("timestamp", strconv.FormatInt(fapiNowMs(c), 10))
	v.Set("recvWindow", "5000")
	qs := v.Encode()
	sig := fapiSign(qs, c.SecretKey)
	u := strings.TrimRight(c.BaseURL, "/") + path + "?" + qs + "&signature=" + sig
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-MBX-APIKEY", c.APIKey)
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, parseAPIErr(b)
	}
	if err := checkFapiBodyError(b); err != nil {
		return nil, err
	}
	return b, nil
}

func fapiDeleteSigned(ctx context.Context, c *futures.Client, path string, v url.Values) ([]byte, error) {
	if v == nil {
		v = url.Values{}
	}
	v.Set("timestamp", strconv.FormatInt(fapiNowMs(c), 10))
	v.Set("recvWindow", "5000")
	qs := v.Encode()
	sig := fapiSign(qs, c.SecretKey)
	u := strings.TrimRight(c.BaseURL, "/") + path + "?" + qs + "&signature=" + sig
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-MBX-APIKEY", c.APIKey)
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, parseAPIErr(b)
	}
	if err := checkFapiBodyError(b); err != nil {
		return nil, err
	}
	return b, nil
}

func parseAPIErr(b []byte) error {
	var e struct {
		Code int64  `json:"code"`
		Msg  string `json:"msg"`
	}
	if json.Unmarshal(b, &e) == nil && e.Msg != "" {
		return fmt.Errorf("<APIError> code=%d, msg=%s", e.Code, e.Msg)
	}
	return fmt.Errorf("HTTP %s", string(b))
}

// PlaceAlgoTPSLLong выставляет TP и SL для лонга (close all) через /fapi/v1/algoOrder.
func PlaceAlgoTPSLLong(ctx context.Context, c *futures.Client, symbol string, tp, sl float64) error {
	tpS := FormatStopPrice(symbol, tp)
	slS := FormatStopPrice(symbol, sl)
	// TP: SELL TAKE_PROFIT_MARKET — цена >= trigger
	v := url.Values{}
	v.Set("algoType", "CONDITIONAL")
	v.Set("symbol", symbol)
	v.Set("side", "SELL")
	v.Set("type", "TAKE_PROFIT_MARKET")
	v.Set("triggerPrice", tpS)
	v.Set("closePosition", "true")
	v.Set("workingType", "CONTRACT_PRICE")
	if _, err := fapiPostForm(ctx, c, "/fapi/v1/algoOrder", v); err != nil {
		return fmt.Errorf("algo TP: %w", err)
	}
	v2 := url.Values{}
	v2.Set("algoType", "CONDITIONAL")
	v2.Set("symbol", symbol)
	v2.Set("side", "SELL")
	v2.Set("type", "STOP_MARKET")
	v2.Set("triggerPrice", slS)
	v2.Set("closePosition", "true")
	v2.Set("workingType", "CONTRACT_PRICE")
	if _, err := fapiPostForm(ctx, c, "/fapi/v1/algoOrder", v2); err != nil {
		return fmt.Errorf("algo SL: %w", err)
	}
	return nil
}

// PlaceAlgoTPSLShort — TP и SL для шорта.
func PlaceAlgoTPSLShort(ctx context.Context, c *futures.Client, symbol string, tp, sl float64) error {
	tpS := FormatStopPrice(symbol, tp)
	slS := FormatStopPrice(symbol, sl)
	v := url.Values{}
	v.Set("algoType", "CONDITIONAL")
	v.Set("symbol", symbol)
	v.Set("side", "BUY")
	v.Set("type", "TAKE_PROFIT_MARKET")
	v.Set("triggerPrice", tpS)
	v.Set("closePosition", "true")
	v.Set("workingType", "CONTRACT_PRICE")
	if _, err := fapiPostForm(ctx, c, "/fapi/v1/algoOrder", v); err != nil {
		return fmt.Errorf("algo TP: %w", err)
	}
	v2 := url.Values{}
	v2.Set("algoType", "CONDITIONAL")
	v2.Set("symbol", symbol)
	v2.Set("side", "BUY")
	v2.Set("type", "STOP_MARKET")
	v2.Set("triggerPrice", slS)
	v2.Set("closePosition", "true")
	v2.Set("workingType", "CONTRACT_PRICE")
	if _, err := fapiPostForm(ctx, c, "/fapi/v1/algoOrder", v2); err != nil {
		return fmt.Errorf("algo SL: %w", err)
	}
	return nil
}

// CancelAllOpenAlgoOrders снимает CONDITIONAL algo по символу (для трейлинга / PANIC).
func CancelAllOpenAlgoOrders(ctx context.Context, c *futures.Client, symbol string) error {
	b, err := fapiGetSigned(ctx, c, "/fapi/v1/openAlgoOrders", url.Values{"symbol": {symbol}})
	if err != nil {
		return err
	}
	var orders []struct {
		AlgoId int64 `json:"algoId"`
	}
	if err := json.Unmarshal(b, &orders); err != nil {
		return nil
	}
	for _, o := range orders {
		if o.AlgoId == 0 {
			continue
		}
		q := url.Values{}
		q.Set("algoId", strconv.FormatInt(o.AlgoId, 10))
		_, _ = fapiDeleteSigned(ctx, c, "/fapi/v1/algoOrder", q)
	}
	return nil
}
