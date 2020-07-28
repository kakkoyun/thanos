// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"time"

	"github.com/go-kit/kit/log"
	"golang.org/x/time/rate"
)

type rateLimitedLogger struct {
	logger     log.Logger
	limiter    *rate.Limiter
	occurrence int
}

func (r *rateLimitedLogger) Log(keyvals ...interface{}) error {
	r.occurrence++
	if r.limiter.Allow() {
		return log.With(r.logger, "occurrence", r.occurrence).Log(keyvals...)
	}
	return nil
}

// Limit creates a new logger with rate limiting using given time window and burst parameters.
func Limit(logger log.Logger, every time.Duration, b int) log.Logger {
	return &rateLimitedLogger{logger: logger, limiter: rate.NewLimiter(rate.Every(every), b)}
}
