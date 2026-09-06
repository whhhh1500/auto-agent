package runtime

import "fmt"

// ValidateEffectDescriptor is the canonical validation boundary for effect
// descriptors. Adapters must call it before persisting or replaying effects.
func ValidateEffectDescriptor(descriptor EffectDescriptor) error {
	if err := validateID(string(descriptor.ID), "effect"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidEffect, err)
	}
	if err := validateID(string(descriptor.ModuleID), "effect module"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidEffect, err)
	}
	if err := validateID(descriptor.CompositionRevision, "effect composition"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidEffect, err)
	}
	if !descriptor.ModuleRevision.Valid() || !descriptor.Phase.Valid() {
		return fmt.Errorf("%w: invalid effect revision or phase", ErrInvalidEffect)
	}
	if err := validateAction(descriptor.Forward); err != nil {
		return fmt.Errorf("%w: forward action: %v", ErrInvalidEffect, err)
	}
	if err := validateAction(descriptor.Inverse); err != nil {
		return fmt.Errorf("%w: inverse action: %v", ErrInvalidEffect, err)
	}
	return nil
}

func validateAction(action EffectAction) error {
	if err := validateID(action.Kind, "effect action kind"); err != nil {
		return err
	}
	if err := validateID(action.Target, "effect action target"); err != nil {
		return err
	}
	if len(action.Payload) > 1<<20 {
		return fmt.Errorf("effect action payload is too large")
	}
	return nil
}
