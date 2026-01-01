package hyperloglog

import (
	"crypto/rand"
	"fmt"
	"testing"
)

/*
 * Micro-benchmarks for the HyperLogLog data structure.
 *
 * These benchmarks measure the raw performance of the HLL implementation
 * in isolation, without network or serialization overhead. They are useful
 * for identifying performance regressions and understanding the cost of
 * individual operations.
 *
 * Run with: go test -bench=. -benchmem ./internal/pds/hyperloglog/
 * Profile with: go test -bench=BenchmarkHLL -cpuprofile=cpu.prof ./internal/pds/hyperloglog/
 */

/*
 * Generates a slice of random byte slices for use in benchmarks.
 * Each element is 16 bytes of cryptographically random data, which ensures
 * a uniform distribution of hash values across the register space.
 */
func generateRandomElements(count int) [][]byte {
	elements := make([][]byte, count)
	for i := 0; i < count; i++ {
		elements[i] = make([]byte, 16)
		_, _ = rand.Read(elements[i])
	}
	return elements
}

/*
 * Benchmarks the Add operation in sparse mode.
 * Sparse mode is used when the HLL has few non-zero registers. It uses a
 * sorted list of (index, value) pairs, which requires binary search on each
 * add. This benchmark measures the cost of that search and potential insertion.
 */
func BenchmarkHLL_Add_Sparse(b *testing.B) {
	/*
	 * DESIGN
	 * ------
	 *
	 * We create a fresh HLL for each benchmark iteration to ensure we stay
	 * in sparse mode throughout. The elements are pre-generated to avoid
	 * measuring the cost of random number generation.
	 *
	 * We use b.ResetTimer() after setup to exclude initialization costs
	 * from the measurement.
	 */
	elements := generateRandomElements(b.N)

	b.ResetTimer()
	b.ReportAllocs()

	hll := New()
	for i := 0; i < b.N; i++ {
		hll.Add(elements[i%len(elements)])

		/* Reset HLL periodically to stay in sparse mode. The conversion
		 * threshold is 750 non-zero registers, so we reset every 500 adds
		 * to ensure we never convert to dense during this benchmark. */
		if i%500 == 499 {
			hll = New()
		}
	}
}

/*
 * Benchmarks the Add operation in dense mode.
 * Dense mode uses a fixed 16KB byte array. Each Add requires computing
 * a hash, extracting the register index, reading the current register value,
 * and potentially writing a new value if the rank is higher.
 */
func BenchmarkHLL_Add_Dense(b *testing.B) {
	/*
	 * DESIGN
	 * ------
	 *
	 * We pre-populate the HLL with enough elements to trigger conversion
	 * to dense mode before starting the benchmark. This ensures we are
	 * measuring only dense mode performance.
	 */
	hll := New()

	/* Force conversion to dense by adding enough unique elements to exceed
	 * the sparse-to-dense threshold (750 non-zero registers). */
	setupElements := generateRandomElements(1000)
	for _, elem := range setupElements {
		hll.Add(elem)
	}

	/* Verify we're in dense mode. */
	if hll.header.encoding != dense {
		b.Fatal("HLL should be in dense mode after setup")
	}

	elements := generateRandomElements(b.N)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		hll.Add(elements[i%len(elements)])
	}
}

/*
 * Benchmarks the Count operation when the cache is valid.
 * This represents the fast path where the cardinality has already been
 * computed and cached. It should be extremely fast as it only requires
 * acquiring a read lock and returning the cached value.
 */
func BenchmarkHLL_Count_Cached(b *testing.B) {
	hll := New()

	/* Add some elements and call Count once to populate the cache. */
	elements := generateRandomElements(100)
	for _, elem := range elements {
		hll.Add(elem)
	}
	hll.Count() // Populate cache

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		hll.Count()
	}
}

/*
 * Benchmarks the Count operation when the cache is invalid.
 * This represents the slow path where the cardinality must be recomputed
 * by iterating over all 16,384 registers and applying the Ertl formula.
 */
func BenchmarkHLL_Count_Uncached(b *testing.B) {
	/*
	 * DESIGN
	 * ------
	 *
	 * To ensure the cache is always invalid, we add a new element before
	 * each Count call. This forces recomputation every time and measures
	 * the true cost of the Ertl algorithm.
	 */
	hll := New()

	/* Force to dense mode for consistent measurements. */
	setupElements := generateRandomElements(1000)
	for _, elem := range setupElements {
		hll.Add(elem)
	}

	elements := generateRandomElements(b.N)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		hll.Add(elements[i%len(elements)]) // Invalidate cache
		hll.Count()
	}
}

/*
 * Benchmarks serialization of a sparse HLL.
 * Sparse serialization involves encoding the header plus a variable-length
 * list of (index, value) pairs.
 */
func BenchmarkHLL_Serialize_Sparse(b *testing.B) {
	hll := New()

	/* Add a moderate number of elements to have some data, but stay sparse. */
	elements := generateRandomElements(200)
	for _, elem := range elements {
		hll.Add(elem)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		hll.Serialize()
	}
}

/*
 * Benchmarks serialization of a dense HLL.
 * Dense serialization involves encoding the header plus the fixed 16KB
 * register array.
 */
func BenchmarkHLL_Serialize_Dense(b *testing.B) {
	hll := New()

	/* Force to dense mode. */
	elements := generateRandomElements(1000)
	for _, elem := range elements {
		hll.Add(elem)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		hll.Serialize()
	}
}

/*
 * Benchmarks deserialization of a sparse HLL.
 */
func BenchmarkHLL_Deserialize_Sparse(b *testing.B) {
	hll := New()

	elements := generateRandomElements(200)
	for _, elem := range elements {
		hll.Add(elem)
	}

	data := hll.Serialize()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = Deserialize(data)
	}
}

/*
 * Benchmarks deserialization of a dense HLL.
 */
func BenchmarkHLL_Deserialize_Dense(b *testing.B) {
	hll := New()

	elements := generateRandomElements(1000)
	for _, elem := range elements {
		hll.Add(elem)
	}

	data := hll.Serialize()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = Deserialize(data)
	}
}

/*
 * Benchmarks the GetCachedCount fast path.
 * This function peeks at the serialized header without full deserialization,
 * useful for the server to avoid allocation when the cache is valid.
 */
func BenchmarkHLL_GetCachedCount(b *testing.B) {
	hll := New()

	elements := generateRandomElements(100)
	for _, elem := range elements {
		hll.Add(elem)
	}
	hll.Count() // Ensure cache is valid

	data := hll.Serialize()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		GetCachedCount(data)
	}
}

/*
 * Benchmarks the hash-to-index-and-rank calculation.
 * This is the core operation performed on every Add, extracting the register
 * index from the lower bits and the rank from the trailing zeros.
 */
func BenchmarkHLL_HashToIndexAndRank(b *testing.B) {
	elements := generateRandomElements(b.N)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		hashToIndexAndRank(elements[i%len(elements)])
	}
}

/*
 * Benchmarks Add with varying cardinalities to show scaling behavior.
 * This is useful for understanding how performance changes as the HLL fills up.
 */
func BenchmarkHLL_Add_Scaling(b *testing.B) {
	cardinalities := []int{100, 1000, 10000, 100000}

	for _, card := range cardinalities {
		b.Run(fmt.Sprintf("cardinality_%d", card), func(b *testing.B) {
			elements := generateRandomElements(card)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				hll := New()
				for _, elem := range elements {
					hll.Add(elem)
				}
			}
		})
	}
}
