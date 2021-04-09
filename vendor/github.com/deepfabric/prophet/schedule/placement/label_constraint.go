package placement

import (
	"strings"

	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/util/slice"
)

// LabelConstraintOp defines how a LabelConstraint matches a container. It can be one of
// 'in', 'notIn', 'exists', or 'notExists'.
type LabelConstraintOp string

const (
	// In restricts the container label value should in the value list.
	// If label does not exist, `in` is always false.
	In LabelConstraintOp = "in"
	// NotIn restricts the container label value should not in the value list.
	// If label does not exist, `notIn` is always true.
	NotIn LabelConstraintOp = "notIn"
	// Exists restricts the container should have the label.
	Exists LabelConstraintOp = "exists"
	// NotExists restricts the container should not have the label.
	NotExists LabelConstraintOp = "notExists"
)

func validateOp(op LabelConstraintOp) bool {
	return op == In || op == NotIn || op == Exists || op == NotExists
}

// LabelConstraint is used to filter container when trying to place peer of a resource.
type LabelConstraint struct {
	Key    string            `json:"key,omitempty"`
	Op     LabelConstraintOp `json:"op,omitempty"`
	Values []string          `json:"values,omitempty"`
}

// MatchContainer checks if a container matches the constraint.
func (c *LabelConstraint) MatchContainer(container *core.CachedContainer) bool {
	switch c.Op {
	case In:
		label := container.GetLabelValue(c.Key)
		return label != "" && slice.AnyOf(c.Values, func(i int) bool { return c.Values[i] == label })
	case NotIn:
		label := container.GetLabelValue(c.Key)
		return label == "" || slice.NoneOf(c.Values, func(i int) bool { return c.Values[i] == label })
	case Exists:
		return container.GetLabelValue(c.Key) != ""
	case NotExists:
		return container.GetLabelValue(c.Key) == ""
	}
	return false
}

// For backward compatibility. Need to remove later.
var legacyExclusiveLabels = []string{"engine", "exclusive"}

// If a container has exclusiveLabels, it can only be selected when the label is
// explicitly specified in constraints.
func isExclusiveLabel(key string) bool {
	return strings.HasPrefix(key, "$") || slice.AnyOf(legacyExclusiveLabels, func(i int) bool {
		return key == legacyExclusiveLabels[i]
	})
}

// MatchLabelConstraints checks if a container matches label constraints list.
func MatchLabelConstraints(container *core.CachedContainer, constraints []LabelConstraint) bool {
	if container == nil {
		return false
	}

	for _, l := range container.Meta.Labels() {
		if isExclusiveLabel(l.GetKey()) &&
			slice.NoneOf(constraints, func(i int) bool { return constraints[i].Key == l.GetKey() }) {
			return false
		}
	}

	return slice.AllOf(constraints, func(i int) bool { return constraints[i].MatchContainer(container) })
}
