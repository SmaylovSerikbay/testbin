package futures

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
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	UserAgent      = "binance-scalper/1.0"
	DefaultRecvWin = "5000"
)

func baseURL() string {
	if u := strings.TrimSpace(os.Getenv("BINANCE_FAPI_URL")); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "https://fapi.binance.com"
}

// BaseURLForLog возвращает текущий REST base (для диагностики).
func BaseURLForLog() string { return baseURL() }

// StreamBaseForLog — база WebSocket из BINANCE_FSTREAM_WS или продакшен.
func StreamBaseForLog() string {
	if u := strings.TrimSpace(os.Getenv("BINANCE_FSTREAM_WS")); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "wss://fstream.binance.com"
}

// PingServerTime проверяет доступность REST (GET /fapi/v1/time).
func PingServerTime(ctx context.Context) error {
	cl := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL()+"/fapi/v1/time", nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", UserAgent)
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET /fapi/v1/time: %d %s", resp.StatusCode, string(body))
	}
	return nil
}

type Client struct {
	Key    string
	Secret string
	HTTP   *http.Client
}

func NewClient(key, secret string) *Client {
	return &Client{
		Key:    key,
		Secret: secret,
		HTTP:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) sign(query string) string {
	mac := hmac.New(sha256.New, []byte(c.Secret))
	mac.Write([]byte(query))
	return hex.EncodeToString(mac.Sum(nil))
}

func (c *Client) doSigned(ctx context.Context, method, path string, params url.Values) ([]byte, int, error) {
	if params == nil {
		params = url.Values{}
	}
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("recvWindow", DefaultRecvWin)
	qs := params.Encode()
	sig := c.sign(qs)
	full := baseURL() + path + "?" + qs + "&signature=" + sig

	req, err := http.NewRequestWithContext(ctx, method, full, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("X-MBX-APIKEY", c.Key)
	req.Header.Set("User-Agent", UserAgent)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body, resp.StatusCode, nil
}

func (c *Client) doPublic(ctx context.Context, method, path string, params url.Values) ([]byte, int, error) {
	u := baseURL() + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", UserAgent)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body, resp.StatusCode, nil
}

func (c *Client) SetMarginType(ctx context.Context, symbol, marginType string) error {
	v := url.Values{}
	v.Set("symbol", symbol)
	v.Set("marginType", marginType)
	body, code, err := c.doSigned(ctx, http.MethodPost, "/fapi/v1/marginType", v)
	if err != nil {
		return err
	}
	if code != 200 {
		s := string(body)
		if strings.Contains(s, "No need to change margin type") {
			return nil
		}
		return fmt.Errorf("marginType %d: %s", code, s)
	}
	return nil
}

func (c *Client) SetLeverage(ctx context.Context, symbol string, leverage int) error {
	v := url.Values{}
	v.Set("symbol", symbol)
	v.Set("leverage", strconv.Itoa(leverage))
	body, code, err := c.doSigned(ctx, http.MethodPost, "/fapi/v1/leverage", v)
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("leverage %d: %s", code, string(body))
	}
	return nil
}

func (c *Client) MarketOrder(ctx context.Context, symbol, side string, quantity float64) (json.RawMessage, error) {
	v := url.Values{}
	v.Set("symbol", symbol)
	v.Set("side", side)
	v.Set("type", "MARKET")
	v.Set("quantity", formatQty(quantity))
	body, code, err := c.doSigned(ctx, http.MethodPost, "/fapi/v1/order", v)
	if err != nil {
		return nil, err
	}
	if code == 429 || code == 418 {
		return nil, &RateLimitError{Code: code, Body: string(body)}
	}
	if code != 200 {
		return nil, fmt.Errorf("order %d: %s", code, string(body))
	}
	return json.RawMessage(body), nil
}

func (c *Client) StartUserStream(ctx context.Context) (string, error) {
	body, code, err := c.doSigned(ctx, http.MethodPost, "/fapi/v1/listenKey", url.Values{})
	if err != nil {
		return "", err
	}
	if code != 200 {
		return "", fmt.Errorf("listenKey %d: %s", code, string(body))
	}
	var out struct {
		ListenKey string `json:"listenKey"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", err
	}
	return out.ListenKey, nil
}

func (c *Client) KeepAliveUserStream(ctx context.Context, listenKey string) error {
	v := url.Values{}
	v.Set("listenKey", listenKey)
	body, code, err := c.doSigned(ctx, http.MethodPut, "/fapi/v1/listenKey", v)
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("keepalive %d: %s", code, string(body))
	}
	return nil
}

func (c *Client) PositionRisk(ctx context.Context, symbol string) ([]PositionRisk, error) {
	v := url.Values{}
	if symbol != "" {
		v.Set("symbol", symbol)
	}
	body, code, err := c.doSigned(ctx, http.MethodGet, "/fapi/v2/positionRisk", v)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("positionRisk %d: %s", code, string(body))
	}
	var pr []PositionRisk
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, err
	}
	return pr, nil
}

type PositionRisk struct {
	Symbol           string `json:"symbol"`
	PositionAmt      string `json:"positionAmt"`
	EntryPrice       string `json:"entryPrice"`
	UnRealizedProfit string `json:"unRealizedProfit"`
	Leverage         string `json:"leverage"`
}

type RateLimitError struct {
	Code int
	Body string
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limit %d: %s", e.Code, e.Body)
}

// Klines returns closed candle closes (oldest first), interval e.g. "1m", limit <= 1500.
func (c *Client) Klines(ctx context.Context, symbol, interval string, limit int) ([]float64, error) {
	v := url.Values{}
	v.Set("symbol", symbol)
	v.Set("interval", interval)
	v.Set("limit", strconv.Itoa(limit))
	body, code, err := c.doPublic(ctx, http.MethodGet, "/fapi/v1/klines", v)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("klines %d: %s", code, string(body))
	}
	var raw [][]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	out := make([]float64, 0, len(raw))
	for _, row := range raw {
		if len(row) < 5 {
			continue
		}
		var close float64
		if err := json.Unmarshal(row[4], &close); err != nil {
			var cs string
			if err2 := json.Unmarshal(row[4], &cs); err2 != nil {
				continue
			}
			fmt.Sscanf(cs, "%f", &close)
		}
		out = append(out, close)
	}
	return out, nil
}

func formatQty(q float64) string {
	s := strconv.FormatFloat(q, 'f', -1, 64)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	if s == "" {
		s = "0"
	}
	return s
}
