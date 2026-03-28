package hft

import (
	"os"
	"strings"

	"github.com/adshao/go-binance/v2/futures"
)

// ApplyBinanceEndpoints настраивает REST/WS для go-binance futures.
// Tokyo: отдельного fapi-хоста ap-northeast-1 у Binance нет — на AWS Tokyo используйте
// стандартные endpoint’ы (маршрут до ближайшего узла). Переопределение через env.
func ApplyBinanceEndpoints() {
	region := strings.ToLower(strings.TrimSpace(os.Getenv("BINANCE_REGION")))
	if region == "tokyo" || region == "ap-northeast-1" {
		// Продакшен USDT-M (рекомендуемый вариант для инстанса в Tokyo)
		futures.BaseApiMainUrl = "https://fapi.binance.com"
		futures.BaseWsMainUrl = "wss://fstream.binance.com/ws"
		futures.BaseCombinedMainURL = "wss://fstream.binance.com/stream?streams="
	}

	if strings.EqualFold(strings.TrimSpace(os.Getenv("BINANCE_USE_DEMO")), "true") ||
		strings.EqualFold(strings.TrimSpace(os.Getenv("BINANCE_DEMO")), "1") {
		futures.BaseApiMainUrl = "https://demo-fapi.binance.com"
		futures.BaseWsMainUrl = "wss://fstream.binancefuture.com/ws"
		futures.BaseCombinedMainURL = "wss://fstream.binancefuture.com/stream?streams="
	}

	if u := strings.TrimSpace(os.Getenv("BINANCE_FAPI_URL")); u != "" {
		futures.BaseApiMainUrl = strings.TrimRight(u, "/")
	}
	if u := strings.TrimSpace(os.Getenv("BINANCE_FSTREAM_WS")); u != "" {
		u = strings.TrimRight(u, "/")
		// env задаёт базу без /ws — добавляем суффикс как в библиотеке
		if !strings.HasSuffix(u, "/ws") {
			u = u + "/ws"
		}
		futures.BaseWsMainUrl = u
		// combined: тот же хост, путь /stream?streams=
		base := strings.TrimSuffix(u, "/ws")
		futures.BaseCombinedMainURL = base + "/stream?streams="
	}
	refreshNotionalFloorForOrders()
}
