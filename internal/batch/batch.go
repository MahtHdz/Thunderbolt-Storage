package batch

import (
	"context"
	"fmt"
	"sync"
)

const MaxWorkers = 256

type Job[T any] struct {
	Index int
	Value T
}

type Result[T, R any] struct {
	Index  int
	Value  T
	Output R
	Err    error
}

func Run[T, R any](ctx context.Context, workers int, values []T, fn func(context.Context, T) (R, error)) ([]Result[T, R], error) {
	if workers <= 0 || workers > MaxWorkers {
		return nil, fmt.Errorf("workers must be between 1 and 256")
	}
	if len(values) == 0 {
		return []Result[T, R]{}, nil
	}
	if workers > len(values) {
		workers = len(values)
	}

	jobs := make(chan Job[T])
	results := make(chan Result[T, R], len(values))
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for job := range jobs {
			if err := ctx.Err(); err != nil {
				results <- Result[T, R]{Index: job.Index, Value: job.Value, Err: err}
				continue
			}
			output, err := fn(ctx, job.Value)
			results <- Result[T, R]{Index: job.Index, Value: job.Value, Output: output, Err: err}
		}
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker()
	}

	go func() {
		defer close(jobs)
		for i, value := range values {
			select {
			case <-ctx.Done():
				// Still enqueue remaining jobs so callers receive one ordered result per input.
				for j := i; j < len(values); j++ {
					jobs <- Job[T]{Index: j, Value: values[j]}
				}
				return
			case jobs <- Job[T]{Index: i, Value: value}:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	ordered := make([]Result[T, R], len(values))
	var errs []error
	for result := range results {
		ordered[result.Index] = result
		if result.Err != nil {
			errs = append(errs, result.Err)
		}
	}

	return ordered, joinErrors(errs)
}

func joinErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return multiError(errs)
}

type multiError []error

func (m multiError) Error() string {
	return fmt.Sprintf("%d operation(s) failed", len(m))
}

func (m multiError) Unwrap() []error { return []error(m) }
