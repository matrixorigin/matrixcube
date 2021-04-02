package slice

import "reflect"

// AnyOf returns true if any element in the slice matches the predict func.
func AnyOf(s interface{}, p func(int) bool) bool {
	l := reflect.ValueOf(s).Len()
	for i := 0; i < l; i++ {
		if p(i) {
			return true
		}
	}
	return false
}

// NoneOf returns true if no element in the slice matches the predict func.
func NoneOf(s interface{}, p func(int) bool) bool {
	return !AnyOf(s, p)
}

// AllOf returns true if all elements in the slice match the predict func.
func AllOf(s interface{}, p func(int) bool) bool {
	np := func(i int) bool {
		return !p(i)
	}
	return NoneOf(s, np)
}
