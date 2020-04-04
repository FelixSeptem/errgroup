package errgroup_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/FelixSeptem/errgroup"
)

var (
	Web   = fakeSearch("web")
	Image = fakeSearch("image")
	Video = fakeSearch("video")
)

type Result string
type Search func(ctx context.Context, query string) (Result, error)

func fakeSearch(kind string) Search {
	return func(_ context.Context, query string) (Result, error) {
		return Result(fmt.Sprintf("%s result for %q", kind, query)), nil
	}
}

func ExampleGroup_justErrors() {
	g, _ := errgroup.NewGroupWithContext(
		context.Background(),
		1,
		true,
		&errgroup.RetryOption{
			Mode:       errgroup.Constant,
			Interval:   time.Millisecond * 30,
			MaxRetries: 3,
		},
		3)
	var urls = []string{
		"http://www.golang.org/",
		"http://www.google.com/",
		"http://www.somestupidname.com/",
	}
	for _, url := range urls {
		url := url
		g.Go(func() error {
			// Fetch the URL.
			resp, err := http.Get(url)
			if err == nil {
				resp.Body.Close()
			}
			return err
		})
	}
	if err := g.Wait(); len(err) == 0 {
		fmt.Println("Successfully fetched all URLs.")
	}
}

func ExampleGroup_parallel() {
	Google := func(ctx context.Context, query string) ([]Result, error) {
		g, ctx := errgroup.NewGroupWithContext(
			ctx,
			1,
			true,
			&errgroup.RetryOption{
				Mode:       errgroup.Constant,
				Interval:   time.Millisecond * 30,
				MaxRetries: 3,
			},
			3)

		searches := []Search{Web, Image, Video}
		results := make([]Result, len(searches))
		for i, search := range searches {
			i, search := i, search
			g.Go(func() error {
				result, err := search(ctx, query)
				if err == nil {
					results[i] = result
				}
				return err
			})
		}
		if err := g.Wait(); len(err) > 0 {
			return nil, <-err
		}
		return results, nil
	}

	results, err := Google(context.Background(), "golang")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	for _, result := range results {
		fmt.Println(result)
	}
}

func TestZeroGroup(t *testing.T) {
	err1 := errors.New("errgroup_test: 1")
	err2 := errors.New("errgroup_test: 2")

	cases := []struct {
		errs []error
	}{
		{errs: []error{}},
		{errs: []error{nil}},
		{errs: []error{err1}},
		{errs: []error{err1, nil}},
		{errs: []error{err1, nil, err2}},
	}

	for _, tc := range cases {
		g, _ := errgroup.NewGroupWithContext(
			context.Background(),
			1,
			true,
			&errgroup.RetryOption{
				Mode:       errgroup.Constant,
				Interval:   time.Millisecond * 30,
				MaxRetries: 3,
			},
			3)

		var firstErr error
		for i, err := range tc.errs {
			err := err
			g.Go(func() error { return err })

			if firstErr == nil && err != nil {
				firstErr = err
			}

			if gErr := g.Wait(); len(gErr) > 0 && <-gErr != firstErr {
				t.Errorf("after %T.Go(func() error { return err }) for err in %v\n"+
					"g.Wait() = %v; want %v",
					g, tc.errs[:i+1], err, firstErr)
			}
		}
	}
}

func TestWithContext(t *testing.T) {
	errDoom := errors.New("group_test: doomed")

	cases := []struct {
		errs []error
		want error
	}{
		{want: nil},
		{errs: []error{nil}, want: nil},
		{errs: []error{errDoom}, want: errDoom},
		{errs: []error{errDoom, nil}, want: errDoom},
	}

	for _, tc := range cases {
		g, ctx := errgroup.NewGroupWithContext(
			context.Background(),
			10,
			true,
			&errgroup.RetryOption{
				Mode:       errgroup.Constant,
				Interval:   time.Millisecond * 30,
				MaxRetries: 3,
			},
			3)
		for _, err := range tc.errs {
			err := err
			g.Go(func() error { return err })
		}
		if err := g.Wait(); len(err) > 0 && <-err != tc.want {
			t.Errorf("after %T.Go(func() error { return err }) for err in %v\n"+
				"g.Wait() = %v; want %v",
				g, tc.errs, err, tc.want)
		}

		canceled := false
		select {
		case <-ctx.Done():
			canceled = true
		default:
		}
		if !canceled {
			t.Errorf("after %T.Go(func() error { return err }) for err in %v\n"+
				"ctx.Done() was not closed",
				g, tc.errs)
		}
	}
}
