// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := Limit(log.NewLogfmtLogger(log.NewSyncWriter(&buf)), time.Millisecond, 1)

	done := make(chan struct{})
	go func() {
		<-time.After(1 * time.Millisecond)
		close(done)
	}()

outer:
	for {
		select {
		case <-done:
			break outer
		default:
			logger.Log("msg", "A")
			logger.Log("msg", "B")
			logger.Log("msg", "C")
		}
	}
	testutil.Equals(t, len(strings.Split(buf.String(), "\n")), 3)
}
