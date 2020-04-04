package errgroup

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"golang.org/x/sync/semaphore"
)

type RetryMode uint8

const (
	// not to retry
	Zero RetryMode = iota
	// use constant time duration mode to retry
	Constant
	// use exponential duration mode to retry
	Exponential
)

// use to retry for every func call
type RetryOption struct {
	// choose mode to your retry mode
	Mode RetryMode
	// only work when choose `Constant` retry mode
	Interval time.Duration
	// max retry times
	MaxRetries int64
	// has wrapped by retry
	wrapped bool
}

type errCh struct {
	errs chan error
	mu   sync.Mutex
}

type group struct {
	ctx     context.Context
	wg      sync.WaitGroup
	cancel  func()
	errOnce sync.Once
	// control whole group's concurrency number
	sema *semaphore.Weighted
	// true mean wait all func return
	waitAll bool
	err     *errCh
	// work for every func call
	retryMode *RetryOption
}

// pass a context to get a new error group
// `maxConcurrency` define max concurrency during whole errgroup life time
// `waitAll` stand for two mode: `true` mean error occurs not trigger ctx's cancel function;`false` will trigger once error occurs
// `retryMode` define three mode of retry: zero, constant, exponential
// `maxErrs` define max err errgroup will return
func NewGroupWithContext(ctx context.Context, maxConcurrency int64, waitAll bool, retryMode *RetryOption, maxErrs int) (*group, context.Context) {
	var (
		sema *semaphore.Weighted
		errs *errCh
	)
	ctx, cancel := context.WithCancel(ctx)
	if maxConcurrency > 0 {
		sema = semaphore.NewWeighted(maxConcurrency)
	}
	if maxErrs > 0 {
		errs = &errCh{
			errs: make(chan error, maxErrs),
			mu:   sync.Mutex{},
		}
	}
	return &group{
		ctx:       ctx,
		wg:        sync.WaitGroup{},
		cancel:    cancel,
		errOnce:   sync.Once{},
		sema:      sema,
		waitAll:   waitAll,
		err:       errs,
		retryMode: retryMode,
	}, ctx
}

// wait all funcs run over (wait mode due to `waitAll` control) return err channel if `maxErrs` > 0
func (g *group) Wait() chan error {
	g.wg.Wait()
	g.cancel()
	if g.err != nil {
		return g.err.errs
	}
	return nil
}

// running unit func
func (g *group) Go(f func() error) {
	var fun func() error
	g.wg.Add(1)
	if g.retryMode != nil {
		fun = func() error {
			var backoffOpt backoff.BackOff
			defer func() {
				g.retryMode.wrapped = true
			}()
			switch g.retryMode.Mode {
			case Zero:
				backoffOpt = NewZeroBackoff(g.retryMode.MaxRetries)
			case Constant:
				backoffOpt = NewConstantBackoff(g.retryMode.MaxRetries, g.retryMode.Interval)
			case Exponential:
				backoffOpt = NewExponentialBackoff(g.retryMode.MaxRetries)
			}
			return backoff.Retry(f, backoffOpt)
		}

	}
	go func() {
		if g.sema != nil {
			err := g.sema.Acquire(g.ctx, 1)
			if err != nil {
				if g.err != nil {
					g.err.mu.Lock()
					if len(g.err.errs)-cap(g.err.errs) > 0 {
						g.err.errs <- err
					}
					g.err.mu.Unlock()
				}
				return
			}
		}

		defer func() {
			if g.sema != nil {
				g.sema.Release(1)
			}
			g.wg.Done()
		}()

		if err := fun(); err != nil {
			if g.err != nil {
				g.err.mu.Lock()
				if len(g.err.errs)-cap(g.err.errs) > 0 {
					g.err.errs <- err
				}
				g.err.mu.Unlock()
			}
		}
		if !g.waitAll {
			g.errOnce.Do(func() {
				g.cancel()
			})
		}
	}()
}
