package app

import (
	"context"
	"time"
)

// pollWithContext periodically calls poll function.
//
// Can be canceled via ctx.
func pollWithContext(ctx context.Context, period time.Duration, poll func(ctx context.Context)) error {
	t := time.NewTicker(period)
	defer t.Stop()

	firstTickTimer := time.NewTimer(0)
	defer firstTickTimer.Stop()

	tickerChan := firstTickTimer.C

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tickerChan:
			tickerChan = t.C
			poll(ctx)
		}
	}
}
