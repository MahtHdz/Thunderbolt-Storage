package batch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestRunPreservesInputOrder(t *testing.T) {
	values := []int{3, 1, 2}
	results, err := Run(context.Background(), 2, values, func(_ context.Context, v int) (int, error) { return v * 10, nil })
	if err != nil {
		t.Fatal(err)
	}
	for i := range values {
		if results[i].Value != values[i] {
			t.Fatalf("index %d got %d want %d", i, results[i].Value, values[i])
		}
		if results[i].Output != values[i]*10 {
			t.Fatalf("output index %d got %d", i, results[i].Output)
		}
	}
}

func TestRunReturnsAggregateError(t *testing.T) {
	boom := errors.New("boom")
	_, err := Run(context.Background(), 2, []int{1, 2}, func(_ context.Context, v int) (struct{}, error) {
		if v == 2 {
			return struct{}{}, boom
		}
		return struct{}{}, nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
}

func TestRunBoundsEmptyAndCancellation(t *testing.T) {
	for _, workers := range []int{0, -1, MaxWorkers + 1} {
		if _, err := Run(context.Background(), workers, []int{1}, func(context.Context, int) (int, error) { return 0, nil }); err == nil {
			t.Fatal("invalid workers")
		}
	}
	results, err := Run(context.Background(), 1, []int{}, func(context.Context, int) (int, error) { t.Fatal("called empty job"); return 0, nil })
	if err != nil || len(results) != 0 {
		t.Fatal(results, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	values := make([]int, 100)
	results, err = Run(ctx, 2, values, func(context.Context, int) (int, error) { t.Fatal("called cancelled job"); return 0, nil })
	if !errors.Is(err, context.Canceled) || len(results) != len(values) || err.Error() != "100 operation(s) failed" {
		t.Fatal(err)
	}
	for i, r := range results {
		if r.Index != i || !errors.Is(r.Err, context.Canceled) {
			t.Fatal(r)
		}
	}
	results, err = Run(context.Background(), 8, []int{1}, func(context.Context, int) (int, error) { return 42, nil })
	if err != nil || results[0].Output != 42 {
		t.Fatal(results, err)
	}
}

func TestRunRespectsConcurrencyLimit(t *testing.T) {
	const workers = 3
	var active, peak atomic.Int32
	entered := make(chan struct{}, workers)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := Run(context.Background(), workers, make([]int, 12), func(context.Context, int) (int, error) {
			n := active.Add(1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			active.Add(-1)
			return 1, nil
		})
		done <- err
	}()
	for i := 0; i < workers; i++ {
		<-entered
	}
	// Release each wave without relying on timing to measure concurrency.
	for wave := 0; wave < 4; wave++ {
		for i := 0; i < workers; i++ {
			release <- struct{}{}
		}
		if wave < 3 {
			for i := 0; i < workers; i++ {
				<-entered
			}
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if peak.Load() != workers || active.Load() != 0 {
		t.Fatalf("peak=%d active=%d", peak.Load(), active.Load())
	}
}
