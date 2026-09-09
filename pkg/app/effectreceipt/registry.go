package effectreceipt

import (
	"reflect"
	"sort"
	"sync"
)

// DriverRegistry is a thread-safe, provider-neutral registry. A DriverRef is
// an exact key: duplicate registration, including a second implementation for
// the same ref, fails closed instead of silently replacing recovery semantics.
type DriverRegistry struct {
	mu      sync.RWMutex
	drivers map[DriverRef]Driver
}

// NewDriverRegistry creates an empty registry for application-owned drivers.
func NewDriverRegistry() *DriverRegistry {
	return &DriverRegistry{drivers: make(map[DriverRef]Driver)}
}

// Register adds one driver under its strict DriverRef. It never probes or
// replaces an existing registration after a duplicate is discovered.
func (r *DriverRegistry) Register(driver Driver) error {
	if r == nil || isNilInterface(driver) {
		return ErrInvalidService
	}
	ref, err := registeredDriverRef(driver)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.drivers == nil {
		r.drivers = make(map[DriverRef]Driver)
	}
	if _, exists := r.drivers[ref]; exists {
		return ErrConflict
	}
	r.drivers[ref] = driver
	return nil
}

// Lookup returns only an exact driver registration. A missing ref is not an
// opportunity to select a default provider.
func (r *DriverRegistry) Lookup(ref DriverRef) (Driver, bool, error) {
	if r == nil {
		return nil, false, ErrInvalidService
	}
	if !validDriverRef(ref) {
		return nil, false, ErrDriver
	}
	r.mu.RLock()
	driver, found := r.drivers[ref]
	r.mu.RUnlock()
	return driver, found, nil
}

// Refs returns a deterministic snapshot for callers that schedule recovery
// pages themselves. It does not expose the registry's mutable map.
func (r *DriverRegistry) Refs() []DriverRef {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	refs := make([]DriverRef, 0, len(r.drivers))
	for ref := range r.drivers {
		refs = append(refs, ref)
	}
	r.mu.RUnlock()
	sort.Slice(refs, func(left, right int) bool {
		if refs[left].ID == refs[right].ID {
			return refs[left].Version < refs[right].Version
		}
		return refs[left].ID < refs[right].ID
	})
	return refs
}

func registeredDriverRef(driver Driver) (ref DriverRef, err error) {
	defer func() {
		if recover() != nil {
			err = ErrDriverPanic
		}
	}()
	ref = driver.Ref()
	if !validDriverRef(ref) {
		return DriverRef{}, ErrDriver
	}
	return ref, nil
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	representation := reflect.ValueOf(value)
	switch representation.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return representation.IsNil()
	default:
		return false
	}
}
