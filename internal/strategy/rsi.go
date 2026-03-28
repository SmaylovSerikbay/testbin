package strategy

// RSI вычисляет RSI по Уайлдеру (period). closes — от старых к новым, длина >= period+1.
func RSI(closes []float64, period int) float64 {
	n := len(closes)
	if n < period+1 || period < 1 {
		return 50
	}
	var gainSum, lossSum float64
	for i := 1; i <= period; i++ {
		ch := closes[i] - closes[i-1]
		if ch >= 0 {
			gainSum += ch
		} else {
			lossSum -= ch
		}
	}
	avgG := gainSum / float64(period)
	avgL := lossSum / float64(period)

	for i := period + 1; i < n; i++ {
		ch := closes[i] - closes[i-1]
		var g, l float64
		if ch >= 0 {
			g = ch
		} else {
			l = -ch
		}
		avgG = (avgG*float64(period-1) + g) / float64(period)
		avgL = (avgL*float64(period-1) + l) / float64(period)
	}
	if avgL == 0 {
		return 100
	}
	rs := avgG / avgL
	return 100 - (100 / (1 + rs))
}
