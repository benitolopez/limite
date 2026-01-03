package cms

import "math"

// DimensionsFromProb calculates optimal CMS dimensions from error parameters.
//
// The epsilon parameter controls the relative error bound. The estimated count
// will be at most (true_count + epsilon * N) where N is the total count of all
// items. Smaller epsilon means higher accuracy but wider tables (more memory).
// Typical values are 0.01 (1% error) or 0.001 (0.1% error).
//
// The delta parameter controls the probability of exceeding the error bound.
// Smaller delta means higher confidence but more rows (deeper tables).
// Typical values are 0.01 (1% probability) or 0.001 (0.1% probability).
//
// The formulas are the standard CMS bounds from the literature:
//
//	width = ceil(e / epsilon)     where e is Euler's number
//	depth = ceil(ln(1 / delta))
//
// Memory usage is HeaderSize + (width * depth * 4) bytes. Some examples:
//
//	epsilon=0.001, delta=0.01  -> width=2719, depth=5 (~54KB)
//	epsilon=0.01, delta=0.01   -> width=272, depth=5 (~5.4KB)
//	epsilon=0.001, delta=0.001 -> width=2719, depth=7 (~76KB)
func DimensionsFromProb(epsilon, delta float64) (width, depth uint32) {
	if epsilon <= 0 {
		epsilon = 0.001
	}
	if delta <= 0 {
		delta = 0.01
	}

	width = uint32(math.Ceil(math.E / epsilon))
	depth = uint32(math.Ceil(math.Log(1 / delta)))

	if width < 1 {
		width = 1
	}
	if depth < 1 {
		depth = 1
	}

	return width, depth
}
