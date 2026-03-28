package hft

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/common"
	"github.com/adshao/go-binance/v2/futures"
)

// orderGate — один глобальный замок на hot-path исполнения (снижает гонки и дубли).
var orderGate sync.Mutex

const (
	Leverage        = 20
	TPPriceMove     = 0.015 // +1.5% цена ≈ +30% ROE @20x
	SLPriceMove     = 0.005 // -0.5% цена ≈ -10% ROE @20x
	TrailROETrigger = 0.15  // 15% ROE
	TrailLockROE    = 0.02  // стоп на уровне BE + 2% ROE
)

// With429Retry выполняет fn с повтором при сетевых/лимитных ошибках.
func With429Retry(ctx context.Context, fn func() error) error {
	var last error
	for i := 0; i < 6; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		last = fn()
		if last == nil {
			return nil
		}
		if retryableAPI(last) {
			time.Sleep(time.Duration(200*(1<<min(i, 5))) * time.Millisecond)
			continue
		}
		break
	}
	return last
}

func retryableAPI(err error) bool {
	if err == nil {
		return false
	}
	var ae *common.APIError
	if errors.As(err, &ae) && ae != nil {
		if ae.Code == -1003 || ae.Code == 429 || strings.Contains(strings.ToLower(ae.Message), "way too many") {
			return true
		}
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "429") || strings.Contains(s, "418") ||
		strings.Contains(s, "timeout") || strings.Contains(s, "connection reset")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func tickFloat(symbol string) float64 {
	r := getRules(symbol)
	if r == nil || r.PriceTick.IsZero() {
		return 1e-8
	}
	f, _ := r.PriceTick.Float64()
	if f <= 0 {
		return 1e-8
	}
	return f
}

// Уровни относительно mark, чтобы избежать -2021 (immediate trigger).
func adjustLongBracketStops(symbol string, mark, tp, sl float64) (float64, float64) {
	if mark <= 0 {
		return tp, sl
	}
	tick := tickFloat(symbol)
	buf := math.Max(tick*4, mark*1e-4)
	if sl >= mark {
		sl = mark - buf
	}
	if tp <= mark {
		tp = mark + buf
	}
	return tp, sl
}

func adjustShortBracketStops(symbol string, mark, tp, sl float64) (float64, float64) {
	if mark <= 0 {
		return tp, sl
	}
	tick := tickFloat(symbol)
	buf := math.Max(tick*4, mark*1e-4)
	if sl <= mark {
		sl = mark + buf
	}
	if tp >= mark {
		tp = mark - buf
	}
	return tp, sl
}

func SetupSymbol(ctx context.Context, c *futures.Client, symbol string) error {
	return With429Retry(ctx, func() error {
		err := c.NewChangeMarginTypeService().Symbol(symbol).MarginType(futures.MarginTypeCrossed).Do(ctx)
		if err != nil && !strings.Contains(err.Error(), "No need to change margin type") {
			return err
		}
		_, err = c.NewChangeLeverageService().Symbol(symbol).Leverage(Leverage).Do(ctx)
		return err
	})
}

// MarketOpenLong открывает лонг и выставляет TP/SL (без логов в hot path).
func MarketOpenLong(ctx context.Context, c *futures.Client, symbol, qty string) error {
	return marketOpen(ctx, c, symbol, futures.SideTypeBuy, qty)
}

// MarketOpenShort открывает шорт и выставляет TP/SL.
func MarketOpenShort(ctx context.Context, c *futures.Client, symbol, qty string) error {
	return marketOpen(ctx, c, symbol, futures.SideTypeSell, qty)
}

func marketOpen(ctx context.Context, c *futures.Client, symbol string, side futures.SideType, qty string) error {
	orderGate.Lock()
	defer orderGate.Unlock()
	return With429Retry(ctx, func() error {
		qtyAdj, err := AdjustMarketQty(ctx, c, symbol, qty)
		if err != nil {
			return err
		}
		_, err = c.NewCreateOrderService().
			Symbol(symbol).
			Side(side).
			Type(futures.OrderTypeMarket).
			Quantity(qtyAdj).
			Do(ctx)
		if err != nil {
			return err
		}
		time.Sleep(350 * time.Millisecond)
		pr, err := c.NewGetPositionRiskService().Symbol(symbol).Do(ctx)
		if err != nil || len(pr) == 0 {
			return fmt.Errorf("position risk: %w", err)
		}
		entry, _ := strconv.ParseFloat(pr[0].EntryPrice, 64)
		amt, _ := strconv.ParseFloat(pr[0].PositionAmt, 64)
		if math.Abs(amt) < 1e-12 {
			return nil
		}
		mark, _ := strconv.ParseFloat(pr[0].MarkPrice, 64)
		_ = CancelAllOpenAlgoOrders(ctx, c, symbol)
		time.Sleep(120 * time.Millisecond)
		qtyStr := FormatReduceQty(symbol, math.Abs(amt))
		if qtyStr == "0" {
			return fmt.Errorf("%s: qty для reduceOnly после округления 0", symbol)
		}
		if amt > 0 {
			return placeLongBracket(ctx, c, symbol, entry, mark, qtyStr)
		}
		return placeShortBracket(ctx, c, symbol, entry, mark, qtyStr)
	})
}

func placeLongBracket(ctx context.Context, c *futures.Client, symbol string, entry, mark float64, qty string) error {
	tp := entry * (1 + TPPriceMove)
	sl := entry * (1 - SLPriceMove)
	tp, sl = adjustLongBracketStops(symbol, mark, tp, sl)
	return With429Retry(ctx, func() error {
		return PlaceAlgoTPSLLong(ctx, c, symbol, tp, sl, qty)
	})
}

func placeShortBracket(ctx context.Context, c *futures.Client, symbol string, entry, mark float64, qty string) error {
	tp := entry * (1 - TPPriceMove)
	sl := entry * (1 + SLPriceMove)
	tp, sl = adjustShortBracketStops(symbol, mark, tp, sl)
	return With429Retry(ctx, func() error {
		return PlaceAlgoTPSLShort(ctx, c, symbol, tp, sl, qty)
	})
}

// ApplyTrailLongAfter15PctROE: отмена алго по символу и SL на BE+2% ROE (цена +0.1%), TP переставляется.
func ApplyTrailLongAfter15PctROE(ctx context.Context, c *futures.Client, symbol string, entry, mark float64) error {
	orderGate.Lock()
	defer orderGate.Unlock()
	if entry <= 0 {
		return nil
	}
	roe := (mark - entry) / entry * float64(Leverage)
	if roe < TrailROETrigger {
		return nil
	}
	lockMove := TrailLockROE / float64(Leverage) // 2% ROE -> 0.1% цены
	stop := entry * (1 + lockMove)
	tp := entry * (1 + TPPriceMove)
	return With429Retry(ctx, func() error {
		_ = CancelAllOpenAlgoOrders(ctx, c, symbol)
		if err := c.NewCancelAllOpenOrdersService().Symbol(symbol).Do(ctx); err != nil {
			return err
		}
		pr, err := c.NewGetPositionRiskService().Symbol(symbol).Do(ctx)
		if err != nil || len(pr) == 0 {
			return err
		}
		amt, _ := strconv.ParseFloat(pr[0].PositionAmt, 64)
		if math.Abs(amt) < 1e-12 {
			return nil
		}
		ent, _ := strconv.ParseFloat(pr[0].EntryPrice, 64)
		mk, _ := strconv.ParseFloat(pr[0].MarkPrice, 64)
		qty := FormatReduceQty(symbol, math.Abs(amt))
		if qty == "0" {
			return nil
		}
		if ent > 0 {
			stop = ent * (1 + lockMove)
			tp = ent * (1 + TPPriceMove)
		}
		tp, stop = adjustLongBracketStops(symbol, mk, tp, stop)
		return PlaceAlgoTPSLLong(ctx, c, symbol, tp, stop, qty)
	})
}

// ApplyTrailShortAfter15PctROE симметрично для шорта.
func ApplyTrailShortAfter15PctROE(ctx context.Context, c *futures.Client, symbol string, entry, mark float64) error {
	orderGate.Lock()
	defer orderGate.Unlock()
	if entry <= 0 {
		return nil
	}
	roe := (entry - mark) / entry * float64(Leverage)
	if roe < TrailROETrigger {
		return nil
	}
	lockMove := TrailLockROE / float64(Leverage)
	stop := entry * (1 - lockMove)
	tp := entry * (1 - TPPriceMove)
	return With429Retry(ctx, func() error {
		_ = CancelAllOpenAlgoOrders(ctx, c, symbol)
		if err := c.NewCancelAllOpenOrdersService().Symbol(symbol).Do(ctx); err != nil {
			return err
		}
		pr, err := c.NewGetPositionRiskService().Symbol(symbol).Do(ctx)
		if err != nil || len(pr) == 0 {
			return err
		}
		amt, _ := strconv.ParseFloat(pr[0].PositionAmt, 64)
		if math.Abs(amt) < 1e-12 {
			return nil
		}
		ent, _ := strconv.ParseFloat(pr[0].EntryPrice, 64)
		mk, _ := strconv.ParseFloat(pr[0].MarkPrice, 64)
		qty := FormatReduceQty(symbol, math.Abs(amt))
		if qty == "0" {
			return nil
		}
		if ent > 0 {
			stop = ent * (1 - lockMove)
			tp = ent * (1 - TPPriceMove)
		}
		tp, stop = adjustShortBracketStops(symbol, mk, tp, stop)
		return PlaceAlgoTPSLShort(ctx, c, symbol, tp, stop, qty)
	})
}

// CloseMarket закрывает позицию по символу (MARKET + closePosition).
func CloseMarket(ctx context.Context, c *futures.Client, symbol string) error {
	orderGate.Lock()
	defer orderGate.Unlock()
	return With429Retry(ctx, func() error {
		pr, err := c.NewGetPositionRiskService().Symbol(symbol).Do(ctx)
		if err != nil || len(pr) == 0 {
			return err
		}
		amt, _ := strconv.ParseFloat(pr[0].PositionAmt, 64)
		if math.Abs(amt) < 1e-12 {
			return nil
		}
		side := futures.SideTypeSell
		if amt < 0 {
			side = futures.SideTypeBuy
		}
		_, err = c.NewCreateOrderService().Symbol(symbol).Side(side).
			Type(futures.OrderTypeMarket).ClosePosition(true).Do(ctx)
		return err
	})
}

// PanicCloseAll закрывает все позиции по списку символов (глобальный стоп).
func PanicCloseAll(ctx context.Context, c *futures.Client, symbols []string) error {
	orderGate.Lock()
	defer orderGate.Unlock()
	for _, sym := range symbols {
		_ = CancelAllOpenAlgoOrders(ctx, c, sym)
		_ = With429Retry(ctx, func() error {
			pr, err := c.NewGetPositionRiskService().Symbol(sym).Do(ctx)
			if err != nil || len(pr) == 0 {
				return err
			}
			amt, _ := strconv.ParseFloat(pr[0].PositionAmt, 64)
			if math.Abs(amt) < 1e-12 {
				return nil
			}
			side := futures.SideTypeSell
			if amt < 0 {
				side = futures.SideTypeBuy
			}
			_, err = c.NewCreateOrderService().Symbol(sym).Side(side).
				Type(futures.OrderTypeMarket).ClosePosition(true).Do(ctx)
			return err
		})
		_ = With429Retry(ctx, func() error {
			return c.NewCancelAllOpenOrdersService().Symbol(sym).Do(ctx)
		})
	}
	return nil
}

