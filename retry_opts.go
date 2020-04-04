package errgroup

import (
	"time"

	"github.com/cenkalti/backoff"
)

var (
	_ backoff.BackOff = (*zeroBackoff)(nil)
	_ backoff.BackOff = (*constantBackoff)(nil)
	_ backoff.BackOff = (*exponentialBackoff)(nil)
)

type concurrenyControl struct {
	maxRetries    int64
	remainRetries int64
}

type zeroBackoff struct {
	cc concurrenyControl
}

func (z *zeroBackoff) NextBackOff() time.Duration {
	if z.cc.remainRetries > 0 {
		z.cc.remainRetries--
		return 0
	}
	return -1
}

func (z *zeroBackoff) Reset() {
	z.cc.remainRetries = z.cc.maxRetries
}

type constantBackoff struct {
	interval time.Duration
	cc       concurrenyControl
}

func (c *constantBackoff) NextBackOff() time.Duration {
	// consume no concurrency condition
	if c.cc.remainRetries > 0 {
		c.cc.remainRetries--
		return c.interval
	}
	return -1
}

func (c *constantBackoff) Reset() {
	c.cc.remainRetries = c.cc.maxRetries
}

type exponentialBackoff struct {
	eb *backoff.ExponentialBackOff
	cc concurrenyControl
}

func (e *exponentialBackoff) NextBackOff() time.Duration {
	// consume no concurrency condition
	if e.cc.remainRetries > 0 {
		e.cc.remainRetries--
		return e.eb.NextBackOff()
	}
	return -1
}

func (e *exponentialBackoff) Reset() {
	e.cc.remainRetries = e.cc.maxRetries
	e.eb.Reset()
}

func NewZeroBackoff(maxRetries int64) *zeroBackoff {
	return &zeroBackoff{
		cc: concurrenyControl{
			maxRetries:    maxRetries,
			remainRetries: maxRetries,
		},
	}
}

func NewConstantBackoff(maxRetries int64, interval time.Duration) *constantBackoff {
	return &constantBackoff{
		interval: interval,
		cc: concurrenyControl{
			maxRetries:    maxRetries,
			remainRetries: maxRetries,
		},
	}
}

func NewExponentialBackoff(maxRetries int64) *exponentialBackoff {
	return &exponentialBackoff{
		eb: backoff.NewExponentialBackOff(),
		cc: concurrenyControl{
			maxRetries:    maxRetries,
			remainRetries: maxRetries,
		},
	}
}
