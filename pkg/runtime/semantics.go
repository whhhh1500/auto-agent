package runtime

import (
	"fmt"
	"sort"
)

func ValidateExtension(extension ProvidedExtension) error {
	if err := validateID(string(extension.ID), "extension"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidExtension, err)
	}
	if !extension.Semantic.Valid() {
		return fmt.Errorf("%w: semantic %q is invalid", ErrInvalidExtension, extension.Semantic)
	}
	if !extension.Version.Valid() {
		return fmt.Errorf("%w: version is invalid", ErrInvalidExtension)
	}
	if err := validateID(string(extension.Contract), "contract"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidExtension, err)
	}
	if err := validateResourceKey(extension.ResourceKey); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidExtension, err)
	}
	seenBefore := make(map[ExtensionID]struct{}, len(extension.Before)+len(extension.After))
	for _, id := range append(append([]ExtensionID(nil), extension.Before...), extension.After...) {
		if err := validateID(string(id), "ordering extension"); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidExtension, err)
		}
		if id == extension.ID {
			return fmt.Errorf("%w: extension cannot order itself", ErrSemanticConflict)
		}
		if _, duplicate := seenBefore[id]; duplicate {
			return fmt.Errorf("%w: duplicate ordering target %q", ErrSemanticConflict, id)
		}
		seenBefore[id] = struct{}{}
	}
	return nil
}

func ValidateCollection(extensions []ProvidedExtension) error {
	return validateSemantic(extensions, SemanticCollection)
}

func ValidatePipeline(extensions []ProvidedExtension) error {
	if err := validateSemantic(extensions, SemanticPipeline); err != nil {
		return err
	}
	byID := make(map[ExtensionID]ProvidedExtension, len(extensions))
	for _, extension := range extensions {
		byID[extension.ID] = extension
	}
	for _, extension := range extensions {
		for _, targetID := range append(append([]ExtensionID(nil), extension.Before...), extension.After...) {
			target, found := byID[targetID]
			if !found {
				return fmt.Errorf("%w: ordering target %q is missing", ErrSemanticConflict, targetID)
			}
			if target.Contract != extension.Contract {
				return fmt.Errorf("%w: pipeline contracts %q and %q are incompatible", ErrSemanticConflict, extension.Contract, target.Contract)
			}
		}
	}
	byContract := make(map[ContractID][]ProvidedExtension)
	for _, extension := range extensions {
		byContract[extension.Contract] = append(byContract[extension.Contract], extension)
	}
	contracts := make([]ContractID, 0, len(byContract))
	for contract := range byContract {
		contracts = append(contracts, contract)
	}
	sort.Slice(contracts, func(i, j int) bool { return contracts[i] < contracts[j] })
	for _, contract := range contracts {
		if len(byContract[contract]) > MaxPipelineGroup {
			return fmt.Errorf("%w: pipeline contract group %q exceeds %d", ErrSemanticConflict, contract, MaxPipelineGroup)
		}
		if _, err := pipelineOrder(byContract[contract]); err != nil {
			return err
		}
	}
	return nil
}

func ValidateScopedOverlay(extensions []ProvidedExtension) error {
	if err := validateSemantic(extensions, SemanticScopedOverlay); err != nil {
		return err
	}
	return rejectDuplicateResourceKeys(extensions)
}

func ValidateSingleStrategy(extensions []ProvidedExtension) error {
	if err := validateSemantic(extensions, SemanticSingleStrategy); err != nil {
		return err
	}
	return rejectDuplicateResourceKeys(extensions)
}

func ValidateStatefulResource(extensions []ProvidedExtension) error {
	if err := validateSemantic(extensions, SemanticStatefulResource); err != nil {
		return err
	}
	return rejectDuplicateResourceKeys(extensions)
}

func ValidateExtensions(semantic ExtensionSemantic, extensions []ProvidedExtension) error {
	switch semantic {
	case SemanticCollection:
		return ValidateCollection(extensions)
	case SemanticPipeline:
		return ValidatePipeline(extensions)
	case SemanticScopedOverlay:
		return ValidateScopedOverlay(extensions)
	case SemanticSingleStrategy:
		return ValidateSingleStrategy(extensions)
	case SemanticStatefulResource:
		return ValidateStatefulResource(extensions)
	default:
		return fmt.Errorf("%w: semantic %q is invalid", ErrSemanticConflict, semantic)
	}
}

func validateSemantic(extensions []ProvidedExtension, semantic ExtensionSemantic) error {
	seen := make(map[ExtensionID]struct{}, len(extensions))
	priorities := make(map[ContractID]map[int]struct{}, len(extensions))
	for _, extension := range extensions {
		if err := ValidateExtension(extension); err != nil {
			return err
		}
		if extension.Semantic != semantic {
			return fmt.Errorf("%w: %q expected %q", ErrSemanticConflict, extension.Semantic, semantic)
		}
		if _, duplicate := seen[extension.ID]; duplicate {
			return fmt.Errorf("%w: duplicate extension %q", ErrSemanticConflict, extension.ID)
		}
		seen[extension.ID] = struct{}{}
		if semantic == SemanticCollection {
			byContract := priorities[extension.Contract]
			if byContract == nil {
				byContract = make(map[int]struct{})
				priorities[extension.Contract] = byContract
			}
			if _, duplicate := byContract[extension.Priority]; duplicate {
				return fmt.Errorf("%w: equal collection priority %d", ErrSemanticConflict, extension.Priority)
			}
			byContract[extension.Priority] = struct{}{}
		}
		if semantic != SemanticPipeline && (len(extension.Before) != 0 || len(extension.After) != 0) {
			return fmt.Errorf("%w: ordering is only valid for pipeline extensions", ErrSemanticConflict)
		}
	}
	return nil
}

func rejectDuplicateResourceKeys(extensions []ProvidedExtension) error {
	seen := make(map[ContractID]map[string]ExtensionID, len(extensions))
	for _, extension := range extensions {
		if extension.ResourceKey == "" {
			return fmt.Errorf("%w: resource key is required for %q", ErrSemanticConflict, extension.ID)
		}
		byKey := seen[extension.Contract]
		if byKey == nil {
			byKey = make(map[string]ExtensionID)
			seen[extension.Contract] = byKey
		}
		if prior, duplicate := byKey[extension.ResourceKey]; duplicate {
			return fmt.Errorf("%w: resource key %q provided by %q and %q", ErrSemanticConflict, extension.ResourceKey, prior, extension.ID)
		}
		byKey[extension.ResourceKey] = extension.ID
	}
	return nil
}

func pipelineOrder(extensions []ProvidedExtension) ([]ProvidedExtension, error) {
	byID := make(map[ExtensionID]ProvidedExtension, len(extensions))
	for _, extension := range extensions {
		byID[extension.ID] = extension
	}
	graph := make(map[ExtensionID]map[ExtensionID]struct{}, len(extensions))
	indegree := make(map[ExtensionID]int, len(extensions))
	for _, extension := range extensions {
		graph[extension.ID] = make(map[ExtensionID]struct{})
	}
	addEdge := func(from, to ExtensionID) error {
		target, ok := byID[to]
		if !ok {
			return fmt.Errorf("%w: ordering target %q is missing", ErrSemanticConflict, to)
		}
		if source := byID[from]; source.Contract != target.Contract {
			return fmt.Errorf("%w: pipeline contracts %q and %q are incompatible", ErrSemanticConflict, source.Contract, target.Contract)
		}
		if _, exists := graph[from][to]; !exists {
			graph[from][to] = struct{}{}
			indegree[to]++
		}
		return nil
	}
	for _, extension := range extensions {
		for _, before := range extension.Before {
			if err := addEdge(extension.ID, before); err != nil {
				return nil, err
			}
		}
		for _, after := range extension.After {
			if err := addEdge(after, extension.ID); err != nil {
				return nil, err
			}
		}
	}
	// A pipeline's priority/id order is only a tie-break among stages whose
	// relative order is already determined. Equal-priority unrelated stages
	// are rejected because silently choosing an ID order would be an implicit
	// conflict policy. A direct or transitive edge is sufficient to disambiguate.
	reachability := pipelineReachability(graph)
	for index, left := range extensions {
		for _, right := range extensions[index+1:] {
			if left.Priority != right.Priority || reachability[left.ID][right.ID] || reachability[right.ID][left.ID] {
				continue
			}
			return nil, fmt.Errorf("%w: equal-priority pipeline stages %q and %q have no declared order", ErrSemanticConflict, left.ID, right.ID)
		}
	}
	ready := make([]ExtensionID, 0, len(extensions))
	for _, extension := range extensions {
		if indegree[extension.ID] == 0 {
			ready = append(ready, extension.ID)
		}
	}
	sortReady := func() {
		sort.Slice(ready, func(i, j int) bool {
			left, right := byID[ready[i]], byID[ready[j]]
			if left.Priority != right.Priority {
				return left.Priority < right.Priority
			}
			return left.ID < right.ID
		})
	}
	sortReady()
	ordered := make([]ProvidedExtension, 0, len(extensions))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		ordered = append(ordered, byID[id])
		for target := range graph[id] {
			indegree[target]--
			if indegree[target] == 0 {
				ready = append(ready, target)
			}
		}
		sortReady()
	}
	if len(ordered) != len(extensions) {
		return nil, fmt.Errorf("%w: pipeline ordering cycle", ErrDependencyCycle)
	}
	return ordered, nil
}

func pipelineReachability(graph map[ExtensionID]map[ExtensionID]struct{}) map[ExtensionID]map[ExtensionID]bool {
	result := make(map[ExtensionID]map[ExtensionID]bool, len(graph))
	for start := range graph {
		seen := make(map[ExtensionID]bool, len(graph))
		stack := []ExtensionID{start}
		for len(stack) != 0 {
			current := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			for next := range graph[current] {
				if seen[next] {
					continue
				}
				seen[next] = true
				stack = append(stack, next)
			}
		}
		result[start] = seen
	}
	return result
}
