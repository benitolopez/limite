package hyperloglog

import (
	"math"
	"testing"
)

// floatEqual is a helper for comparing floats for near-equality.
func floatEqual(a, b float64) bool {
	const tolerance = 1e-12
	if diff := math.Abs(a - b); diff < tolerance {
		return true
	}
	return false
}

func TestErtlHelpers(t *testing.T) {
	t.Run("sigma properties", func(t *testing.T) {
		if !math.IsInf(hllSigma(1.0), 1) {
			t.Errorf("hllSigma(1.0) must be +Inf")
		}
		if hllSigma(0.0) != 0.0 {
			t.Errorf("hllSigma(0.0) must be 0, got %f", hllSigma(0.0))
		}
		if hllSigma(0.5) <= hllSigma(0.4) {
			t.Errorf("hllSigma must be monotonically increasing, but sigma(0.5)=%f <= sigma(0.4)=%f", hllSigma(0.5), hllSigma(0.4))
		}
	})

	t.Run("tau properties", func(t *testing.T) {
		if hllTau(0.0) != 0.0 {
			t.Errorf("hllTau(0.0) must be 0, got %f", hllTau(0.0))
		}
		if hllTau(1.0) != 0.0 {
			t.Errorf("hllTau(1.0) must be 0, got %f", hllTau(1.0))
		}

		expectedTauHalf := 0.1499294958640881
		gotTauHalf := hllTau(0.5)
		if !floatEqual(gotTauHalf, expectedTauHalf) {
			t.Errorf("hllTau(0.5) regression check failed: got %.16f, want %.16f", gotTauHalf, expectedTauHalf)
		}
	})
}
