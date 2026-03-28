package hft

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

// PositionCare — трейлинг +15% ROE, выход при нейтральном PnL 60с.
func PositionCare(ctx context.Context, c *futures.Client, symbols []string, hub *PriceHub, neutralUSDT float64) {
	if neutralUSDT <= 0 {
		neutralUSDT = 0.25
	}
	trailApplied := sync.Map{}
	neutralSince := make(map[string]time.Time)
	t := time.NewTicker(700 * time.Millisecond)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, sym := range symbols {
				pr, err := c.NewGetPositionRiskService().Symbol(sym).Do(ctx)
				if err != nil || len(pr) == 0 {
					continue
				}
				amt, _ := strconv.ParseFloat(pr[0].PositionAmt, 64)
				if math.Abs(amt) < 1e-12 {
					trailApplied.Delete(sym)
					delete(neutralSince, sym)
					continue
				}
				entry, _ := strconv.ParseFloat(pr[0].EntryPrice, 64)
				upnl, _ := strconv.ParseFloat(pr[0].UnRealizedProfit, 64)
				mark := hub.Get(sym)
				if mark <= 0 {
					mark = entry
				}

				if _, done := trailApplied.Load(sym); !done {
					if amt > 0 {
						roe := (mark - entry) / entry * float64(Leverage)
						if roe >= TrailROETrigger {
							if err := ApplyTrailLongAfter15PctROE(ctx, c, sym, entry, mark); err == nil {
								trailApplied.Store(sym, true)
							}
						}
					} else {
						roe := (entry - mark) / entry * float64(Leverage)
						if roe >= TrailROETrigger {
							if err := ApplyTrailShortAfter15PctROE(ctx, c, sym, entry, mark); err == nil {
								trailApplied.Store(sym, true)
							}
						}
					}
				}

				if math.Abs(upnl) < neutralUSDT {
					if t0, ok := neutralSince[sym]; ok {
						if time.Since(t0) >= 60*time.Second {
							_ = CloseMarket(ctx, c, sym)
							delete(neutralSince, sym)
							trailApplied.Delete(sym)
						}
					} else {
						neutralSince[sym] = time.Now()
					}
				} else {
					delete(neutralSince, sym)
				}
			}
		}
	}
}
