package futures

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

// PlaceBracketAfterLong places TP (+tpPct) and SL (-slPct) as fraction of entry (e.g. 0.015 = 1.5%).
func (c *Client) PlaceBracketAfterLong(ctx context.Context, symbol string, entry float64, tpPct, slPct float64) error {
	tpPrice := entry * (1 + tpPct)
	slPrice := entry * (1 - slPct)
	if err := c.placeOrder(ctx, symbol, "SELL", "TAKE_PROFIT_MARKET", tpPrice, true); err != nil {
		return fmt.Errorf("tp: %w", err)
	}
	if err := c.placeOrder(ctx, symbol, "SELL", "STOP_MARKET", slPrice, true); err != nil {
		return fmt.Errorf("sl: %w", err)
	}
	return nil
}

// PlaceBracketAfterShort places TP and SL for a short position.
func (c *Client) PlaceBracketAfterShort(ctx context.Context, symbol string, entry float64, tpPct, slPct float64) error {
	tpPrice := entry * (1 - tpPct)
	slPrice := entry * (1 + slPct)
	if err := c.placeOrder(ctx, symbol, "BUY", "TAKE_PROFIT_MARKET", tpPrice, true); err != nil {
		return fmt.Errorf("tp: %w", err)
	}
	if err := c.placeOrder(ctx, symbol, "BUY", "STOP_MARKET", slPrice, true); err != nil {
		return fmt.Errorf("sl: %w", err)
	}
	return nil
}

func (c *Client) placeOrder(ctx context.Context, symbol, side, typ string, stopPrice float64, closePos bool) error {
	v := url.Values{}
	v.Set("symbol", symbol)
	v.Set("side", side)
	v.Set("type", typ)
	v.Set("stopPrice", formatPrice(stopPrice))
	if closePos {
		v.Set("closePosition", "true")
	}
	body, code, err := c.doSigned(ctx, http.MethodPost, "/fapi/v1/order", v)
	if err != nil {
		return err
	}
	if code == 429 || code == 418 {
		return &RateLimitError{Code: code, Body: string(body)}
	}
	if code != 200 {
		return fmt.Errorf("%s %d: %s", typ, code, string(body))
	}
	return nil
}

func formatPrice(p float64) string {
	return strconv.FormatFloat(p, 'f', -1, 64)
}
