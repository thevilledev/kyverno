package webhook

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/kyverno/kyverno/pkg/cel/compiler"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

type compiledExpression struct {
	expression string
	hash       string
	errors     field.ErrorList
	isValid    bool
	compiledAt time.Time
	isStored   bool
}

type expressionCache struct {
	mu                     sync.RWMutex
	cache                  map[string]*compiledExpression
	preexistingExpressions map[string]bool
}

func NewExpressionCache() *expressionCache {
	return &expressionCache{
		cache:                  make(map[string]*compiledExpression),
		preexistingExpressions: make(map[string]bool),
	}
}

func (c *expressionCache) GetOrCompile(condition admissionregistrationv1.MatchCondition) *compiledExpression {
	hash := c.hashMatchCondition(condition)

	c.mu.RLock()
	if cached, exists := c.cache[hash]; exists {
		c.mu.RUnlock()
		return cached
	}
	isPreexisting := c.preexistingExpressions[condition.Expression]
	c.mu.RUnlock()

	// Build a single-entry map instead of passing the live preexistingExpressions
	// map to the compiler.  Without this, concurrent writes from AddExpression
	// (called on informer goroutines) race with the compiler's map read at
	// pkg/cel/compiler/matchconditions.go:validateMatchConditionsExpression,
	// potentially causing a "concurrent map read and map write" panic.
	//
	// This is safe because the compiler only looks up the expression being
	// compiled (one key per condition) and GetOrCompile always passes exactly
	// one condition.
	preexisting := map[string]bool{condition.Expression: isPreexisting}
	errors := compiler.CompileMatchConditionsWithKubernetesEnv([]admissionregistrationv1.MatchCondition{condition}, preexisting)

	compiled := &compiledExpression{
		expression: condition.Expression,
		hash:       hash,
		errors:     errors,
		isValid:    len(errors) == 0,
		compiledAt: time.Now(),
		isStored:   isPreexisting,
	}

	c.mu.Lock()
	c.cache[hash] = compiled
	c.preexistingExpressions[condition.Expression] = true
	c.mu.Unlock()

	return compiled
}

func (c *expressionCache) FilterValidMatchConditions(conditions []admissionregistrationv1.MatchCondition) []admissionregistrationv1.MatchCondition {
	var validConditions []admissionregistrationv1.MatchCondition

	for _, condition := range conditions {
		compiled := c.GetOrCompile(condition)
		if compiled.isValid {
			validConditions = append(validConditions, condition)
		}
	}

	return validConditions
}

func (c *expressionCache) ValidateMatchConditions(conditions []admissionregistrationv1.MatchCondition) ([]admissionregistrationv1.MatchCondition, field.ErrorList) {
	var validConditions []admissionregistrationv1.MatchCondition
	var allErrors field.ErrorList

	for i, condition := range conditions {
		compiled := c.GetOrCompile(condition)
		if compiled.isValid {
			validConditions = append(validConditions, condition)
		} else {
			for _, err := range compiled.errors {
				allErrors = append(allErrors, field.Invalid(
					field.NewPath("matchConditions").Index(i).Child("expression"),
					condition.Expression,
					err.Detail,
				))
			}
		}
	}

	return validConditions, allErrors
}

func (c *expressionCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*compiledExpression)
	c.preexistingExpressions = make(map[string]bool)
}

func (c *expressionCache) InvalidateOnPolicyChange() {
	c.Invalidate()
}

func (c *expressionCache) AddExpression(condition admissionregistrationv1.MatchCondition) {
	c.mu.Lock()
	c.preexistingExpressions[condition.Expression] = true
	c.mu.Unlock()

	// Compile outside the lock.  See the matching comment in GetOrCompile for
	// why a single-entry map is passed instead of the live preexistingExpressions
	// map.  The value is unconditionally true because we just registered the
	// expression above.
	preexisting := map[string]bool{condition.Expression: true}
	hash := c.hashMatchCondition(condition)
	errors := compiler.CompileMatchConditionsWithKubernetesEnv([]admissionregistrationv1.MatchCondition{condition}, preexisting)

	compiled := &compiledExpression{
		expression: condition.Expression,
		hash:       hash,
		errors:     errors,
		isValid:    len(errors) == 0,
		compiledAt: time.Now(),
		isStored:   true,
	}

	c.mu.Lock()
	c.cache[hash] = compiled
	c.mu.Unlock()
}

func (c *expressionCache) RemoveExpression(condition admissionregistrationv1.MatchCondition) {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := c.hashMatchCondition(condition)
	delete(c.cache, hash)
}

func (c *expressionCache) AddPolicyExpressions(conditions []admissionregistrationv1.MatchCondition) {
	for _, condition := range conditions {
		c.AddExpression(condition)
	}
}

func (c *expressionCache) RemovePolicyExpressions(conditions []admissionregistrationv1.MatchCondition) {
	for _, condition := range conditions {
		c.RemoveExpression(condition)
	}
}

func (c *expressionCache) hashMatchCondition(condition admissionregistrationv1.MatchCondition) string {
	content := fmt.Sprintf("%s:%s", condition.Name, condition.Expression)
	hash := sha256.Sum256([]byte(content))
	return fmt.Sprintf("%x", hash)
}
