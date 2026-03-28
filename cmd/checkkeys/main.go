// Проверка ключей: публичное время + подписанный positionRisk.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"binance-scalper/internal/futures"

	"github.com/joho/godotenv"
)

func main() {
	log.SetFlags(0)
	_ = godotenv.Load()

	key := strings.TrimSpace(os.Getenv("BINANCE_API_KEY"))
	sec := strings.TrimSpace(os.Getenv("BINANCE_API_SECRET"))
	if key == "" || sec == "" {
		log.Fatal("В .env или окружении задайте BINANCE_API_KEY и BINANCE_API_SECRET")
	}

	symbol := strings.TrimSpace(os.Getenv("SYMBOL"))
	if symbol == "" {
		symbol = "BTCUSDT"
	}

	fmt.Println("REST base:", futures.BaseURLForLog())
	fmt.Println("Futures WS base:", futures.StreamBaseForLog())
	fmt.Println("Проверка ключа для символа:", symbol)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := futures.PingServerTime(ctx); err != nil {
		log.Fatalf("Публичный ping: %v", err)
	}
	fmt.Println("OK: сервер отвечает (время синхронизировано).")

	c := futures.NewClient(key, sec)
	pr, err := c.PositionRisk(ctx, symbol)
	if err != nil {
		log.Fatalf("Подписанный запрос positionRisk: %v", err)
	}
	fmt.Printf("OK: API-ключ валиден, позиций по %s: %d\n", symbol, len(pr))
}
