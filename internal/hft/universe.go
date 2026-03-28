package hft

import (
	"context"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/adshao/go-binance/v2/futures"
)

// tradablePerpUSDT — символы со статусом TRADING, PERPETUAL, котируются в USDT.
func tradablePerpUSDT(ctx context.Context, c *futures.Client) (map[string]struct{}, error) {
	info, err := c.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{})
	for _, s := range info.Symbols {
		if s.QuoteAsset != "USDT" {
			continue
		}
		if s.ContractType != futures.ContractTypePerpetual {
			continue
		}
		if s.Status != string(futures.SymbolStatusTypeTrading) {
			continue
		}
		if strings.Contains(s.Symbol, "_") {
			continue
		}
		out[s.Symbol] = struct{}{}
	}
	return out, nil
}

// symbolsWithBook — есть ненулевой bid/ask в стакане (REST), на демо отсекает «пустые» тикеры.
func symbolsWithBook(ctx context.Context, c *futures.Client) (map[string]struct{}, error) {
	tickers, err := c.NewListBookTickersService().Do(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{})
	for _, t := range tickers {
		bp, _ := strconv.ParseFloat(t.BidPrice, 64)
		ap, _ := strconv.ParseFloat(t.AskPrice, 64)
		if bp > 0 && ap > 0 {
			out[t.Symbol] = struct{}{}
		}
	}
	return out, nil
}

// ResolveWatchlist: WATCH=... или топ по 24h quoteVolume среди пар, которые реально торгуются и имеют книгу.
// maxN — сколько символов оставить (не «все 400»: на каждый символ 2 WebSocket в воркере).
func ResolveWatchlist(ctx context.Context, c *futures.Client, maxN int, envWatch string) ([]string, error) {
	envWatch = strings.TrimSpace(envWatch)
	if maxN <= 0 {
		maxN = 12
	}

	perp, err := tradablePerpUSDT(ctx, c)
	if err != nil {
		return nil, err
	}
	book, err := symbolsWithBook(ctx, c)
	if err != nil {
		return nil, err
	}

	pick := func(sym string) bool {
		_, ok1 := perp[sym]
		_, ok2 := book[sym]
		return ok1 && ok2
	}

	if envWatch != "" {
		var out []string
		for _, p := range strings.Split(envWatch, ",") {
			s := strings.ToUpper(strings.TrimSpace(p))
			if s == "" {
				continue
			}
			if pick(s) {
				out = append(out, s)
			} else {
				log.Printf("WATCH: пропуск %s — нет в PERPETUAL/TRADING USDT или пустой bookTicker (часто на демо)", s)
			}
		}
		if len(out) > 0 {
			return out, nil
		}
		return nil, &errNoLiquidSymbols{msg: "ни один символ из WATCH не прошёл фильтр exchangeInfo+bookTicker"}
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
		if !pick(s.Symbol) {
			continue
		}
		qv, _ := strconv.ParseFloat(s.QuoteVolume, 64)
		rows = append(rows, row{s.Symbol, qv})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].qv > rows[j].qv })

	var out []string
	for i := 0; i < len(rows) && len(out) < maxN; i++ {
		out = append(out, rows[i].sym)
	}
	if len(out) == 0 {
		return nil, &errNoLiquidSymbols{msg: "после фильтра PERPETUAL+TRADING+bookTicker список пуст (проверьте endpoint демо/прод)"}
	}
	return out, nil
}

type errNoLiquidSymbols struct {
	msg string
}

func (e *errNoLiquidSymbols) Error() string { return e.msg }
