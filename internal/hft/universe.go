package hft

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/adshao/go-binance/v2/futures"
)

// ResolveWatchlist: WATCH=BTC,ETH или топ по quoteVolume (24h) как стартовая вселенная, не как единственный критерий.
func ResolveWatchlist(ctx context.Context, c *futures.Client, maxN int, envWatch string) ([]string, error) {
	envWatch = strings.TrimSpace(envWatch)
	if envWatch != "" {
		var out []string
		for _, p := range strings.Split(envWatch, ",") {
			s := strings.ToUpper(strings.TrimSpace(p))
			if s != "" {
				out = append(out, s)
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	if maxN <= 0 {
		maxN = 8
	}
	stats, err := c.NewListPriceChangeStatsService().Do(ctx)
	if err != nil {
		return nil, err
	}
	type row struct {
		sym string
		qv  float64
	}
	var rows []row
	for _, s := range stats {
		if !strings.HasSuffix(s.Symbol, "USDT") {
			continue
		}
		if strings.Contains(s.Symbol, "_") {
			continue
		}
		qv, _ := strconv.ParseFloat(s.QuoteVolume, 64)
		rows = append(rows, row{s.Symbol, qv})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].qv > rows[j].qv })
	var out []string
	for i := 0; i < len(rows) && i < maxN; i++ {
		out = append(out, rows[i].sym)
	}
	return out, nil
}
