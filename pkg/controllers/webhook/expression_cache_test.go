package webhook

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
)

// TestGetOrCompileConcurrentWithAddExpression is a regression test for a
// concurrent map read/write panic.  Before the fix, GetOrCompile passed the
// live preexistingExpressions map to the CEL compiler without holding a
// lock.  Concurrent AddExpression calls (driven by informer goroutines)
// wrote to the same map, causing "fatal error: concurrent map read and map
// write".
//
// The fix passes a single-entry map built from a value read under the read
// lock so the compiler never touches the shared map.
//
// Run with: go test -race -run TestGetOrCompileConcurrentWithAddExpression -count=5
func TestGetOrCompileConcurrentWithAddExpression(t *testing.T) {
	cache := NewExpressionCache()

	// Seed the cache so preexistingExpressions is non-empty.
	for i := 0; i < 20; i++ {
		cache.AddExpression(admissionregistrationv1.MatchCondition{
			Name:       fmt.Sprintf("seed-%d", i),
			Expression: fmt.Sprintf("object.metadata.name == 'seed-%d'", i),
		})
	}

	const goroutines = 10
	const iterations = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Half the goroutines call GetOrCompile (workqueue reconcile path).
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				compiled := cache.GetOrCompile(admissionregistrationv1.MatchCondition{
					Name:       fmt.Sprintf("reader-%d-%d", id, i),
					Expression: fmt.Sprintf("object.metadata.name == 'reader-%d-%d'", id, i),
				})
				assert.NotNil(t, compiled)
			}
		}(g)
	}

	// The other half call AddExpression (informer callback path).
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				cache.AddExpression(admissionregistrationv1.MatchCondition{
					Name:       fmt.Sprintf("writer-%d-%d", id, i),
					Expression: fmt.Sprintf("object.metadata.name == 'writer-%d-%d'", id, i),
				})
			}
		}(g)
	}

	wg.Wait()
}

// TestGetOrCompileConcurrentWithInvalidate verifies that Invalidate can run
// concurrently with GetOrCompile without panicking.
func TestGetOrCompileConcurrentWithInvalidate(t *testing.T) {
	cache := NewExpressionCache()

	for i := 0; i < 10; i++ {
		cache.AddExpression(admissionregistrationv1.MatchCondition{
			Name:       fmt.Sprintf("init-%d", i),
			Expression: fmt.Sprintf("object.metadata.name == 'init-%d'", i),
		})
	}

	const goroutines = 5
	const iterations = 30
	var wg sync.WaitGroup
	wg.Add(goroutines + 1)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				compiled := cache.GetOrCompile(admissionregistrationv1.MatchCondition{
					Name:       fmt.Sprintf("inv-%d-%d", id, i),
					Expression: fmt.Sprintf("object.metadata.name == 'inv-%d-%d'", id, i),
				})
				assert.NotNil(t, compiled)
			}
		}(g)
	}

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			cache.Invalidate()
		}
	}()

	wg.Wait()
}
