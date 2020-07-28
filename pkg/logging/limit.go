// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"golang.org/x/time/rate"
)

const messageKey = "msg"

type occurrence struct {
	lastSeen uint32
	current  uint32
}

type rateLimitedLogger struct {
	logger  log.Logger
	limiter *rate.Limiter

	m           sync.Mutex
	occurrences map[string]*occurrence
}

// Log limits the amount of events logged and keep tracks of occurrences of messages.
func (r *rateLimitedLogger) Log(keyvals ...interface{}) error {
	var msg string
	for i := range keyvals {
		if i%2 == 0 && keyvals[i] == messageKey {
			msg = keyvals[i+1].(string)
			r.m.Lock()
			v, ok := r.occurrences[msg]
			if ok {
				v.current++
			} else {
				r.occurrences[msg] = &occurrence{current: 1}
			}
			r.m.Unlock()
			break
		}
	}
	if r.limiter.Allow() {
		if msg != "" {
			r.m.Lock()
			defer r.m.Unlock()

			o := r.occurrences[msg]
			defer func() { o.lastSeen = o.current }()

			return log.With(r.logger, "occurred", o.current, "since last seen", o.current-o.lastSeen).Log(keyvals...)
		}

		return r.logger.Log(keyvals...)
	}

	return nil
}

// Limit creates a new logger with rate limiting using given time window and burst parameters.
func Limit(logger log.Logger, w time.Duration, b int) log.Logger {
	return &rateLimitedLogger{
		logger:      logger,
		limiter:     rate.NewLimiter(rate.Every(w), b),
		occurrences: make(map[string]*occurrence),
	}
}
