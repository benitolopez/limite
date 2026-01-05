Limite - Probabilistic Data Structures Server
===

Limite is a standalone server, written in Go, that provides probabilistic data structures as a network service. Think of it as Redis, but specialized for approximate algorithms that trade perfect accuracy for dramatic memory savings. It runs as a single binary, accepting commands over a raw TCP connection using the Redis RESP protocol.

What are probabilistic data structures?
---

Imagine you're counting unique visitors to a website. The naive approach stores every visitor ID you've seen—if you have 100 million unique visitors, you need to store 100 million IDs. That's gigabytes of memory.

Probabilistic data structures solve this by accepting a small error in exchange for enormous space savings. A HyperLogLog can estimate 100 million unique visitors using just 16KB of memory, with roughly 0.81% standard error. Instead of "exactly 100,000,000 visitors," you get "approximately 99,190,000 to 100,810,000 visitors"—and for most applications, that's good enough.

These structures excel at:

- **Cardinality estimation**: How many unique items have I seen? (HyperLogLog)
- **Membership testing**: Have I seen this item before? (Bloom Filter)
- **Frequency estimation**: How many times have I seen this item? (Count-Min Sketch)
- **Heavy hitter detection**: What are the most frequent items? (Top-K)

They're the workhorses behind real-time analytics, spam detection, network monitoring, and any system that processes more data than can fit in memory.

Limite implements the following probabilistic data structures:

**HyperLogLog** for cardinality estimation:
- Uses 16,384 registers (p=14) for approximately 0.81% standard error
- Dual-mode storage: sparse representation for low cardinality (saves memory), automatic promotion to dense when needed
- Uses the improved Ertl estimator for better accuracy than the original HyperLogLog algorithm
- Memory: ~16KB per key in dense mode

**Bloom Filter** for membership testing:
- Scalable blocked Bloom filter that grows automatically as you add elements
- Cache-efficient: uses 64-byte blocks aligned to CPU cache lines
- Maintains configured false positive rate (default 1%) even as capacity grows

**Count-Min Sketch** for frequency estimation:
- Conservative Update technique reduces over-counting by 50%+ on typical workloads
- Two creation modes: specify dimensions directly, or let Limite calculate optimal dimensions from error bounds
- Typical configuration (width=1000, depth=5) uses ~20KB and achieves ~0.27% error relative to total count

**Top-K** for heavy hitter detection:
- HeavyKeeper algorithm with probabilistic decay
- Efficiently tracks the K most frequent items in a stream
- Pre-computed decay tables eliminate expensive math operations on the hot path
- Configurable K, width, depth, and decay factor

Additionally, Limite provides basic **String** operations (SET, GET, INCR/DECR) for convenience when you need to store auxiliary data alongside your probabilistic structures.

Protocol and compatibility
---

Limite speaks RESP (Redis Serialization Protocol), the same protocol used by Redis. This means:

- You can use `redis-cli` to interact with Limite
- You can use `redis-benchmark` for performance testing
- Existing Redis client libraries work with Limite (just point them at port 6479)
- Commands support both array format (from client libraries) and inline format (for manual testing)

The choice of RESP wasn't arbitrary. RESP is binary-safe, simple to implement, and has extensive tooling. By speaking RESP, Limite becomes immediately accessible from any programming language with a Redis client.

Building and running
---

```
$ git clone https://github.com/benitolopez/limite.git
$ cd limite
$ go build -o limite-server ./cmd/limite-server
$ ./limite-server
```

The server starts on port 6479 by default. You can now connect with redis-cli:

```
$ redis-cli -p 6479
127.0.0.1:6479> PING
PONG
127.0.0.1:6479> HLL.ADD visitors user:1001 user:1002 user:1003
(integer) 1
127.0.0.1:6479> HLL.COUNT visitors
(integer) 3
```

Command line options
---

```
-port int
    TCP server port (default 6479)

-max-conn int
    Maximum concurrent connections (default 100)

-shutdown-timeout duration
    Graceful shutdown timeout (default 5s)

-idle-timeout duration
    Idle client connection timeout, 0 = disabled (default 0)

-hll-sparse-threshold int
    HyperLogLog sparse-to-dense promotion threshold (default 750)

-bf-capacity uint64
    Bloom Filter initial capacity (default 1000)

-bf-error-rate float64
    Bloom Filter target false positive rate (default 0.01)

-aof string
    Append-only file path (default "journal.aof")

-aof-min-size int64
    Minimum file size to trigger automatic compaction (default 64MB)

-aof-rewrite-percent int
    File growth percentage to trigger compaction (default 100)

-aof-load-truncated bool
    Auto-recover from truncated AOF on startup (default true)
```

API reference
---

### Generic commands

**PING**

    PING

Returns `PONG`. Use this to test connectivity.

**DEL**

    DEL key [key ...]

Deletes one or more keys. Returns the number of keys that were removed.

**INFO**

    INFO

Returns server metrics including connection count and commands processed.

**COMPACT**

    COMPACT

Manually triggers AOF compaction. Returns immediately; compaction runs in the background.

**MEMORY**

    MEMORY USAGE key

Returns the approximate number of bytes used by a key and its value. This includes the raw data size plus estimated overhead from Go's internal data structures (~72 bytes per key for map entry bookkeeping).

Returns `nil` if the key doesn't exist.

Example:
```
127.0.0.1:6479> SET mykey "hello world"
OK
127.0.0.1:6479> MEMORY USAGE mykey
(integer) 91
127.0.0.1:6479> HLL.ADD visitors user:1 user:2 user:3
(integer) 1
127.0.0.1:6479> MEMORY USAGE visitors
(integer) 16479
```

---

### String commands

Basic key-value operations for storing auxiliary data.

**SET**

    SET key value

Sets the string value of a key. Returns `OK`.

**GET**

    GET key

Returns the value of key, or nil if the key does not exist.

**INCR / DECR**

    INCR key
    DECR key

Increments or decrements the integer value of a key by one. If the key does not exist, it is set to 0 before the operation. Returns the new value. Returns an error if the value is not an integer or would overflow.

**INCRBY / DECRBY**

    INCRBY key increment
    DECRBY key decrement

Increments or decrements the integer value of a key by the given amount. Returns the new value.

---

### HyperLogLog commands

HyperLogLog provides cardinality estimation—counting unique elements with constant memory usage.

**HLL.ADD**

    HLL.ADD key element [element ...]

Adds elements to the HyperLogLog at `key`. Creates the key if it doesn't exist.

Returns `1` if any internal register was modified (the cardinality estimate changed), `0` otherwise. Note that adding a previously-seen element returns `0`, and adding a new element might also return `0` if it happens to hash to a register that already has a higher value.

Example:
```
127.0.0.1:6479> HLL.ADD pageviews user:1 user:2 user:3
(integer) 1
127.0.0.1:6479> HLL.ADD pageviews user:1
(integer) 0
```

**HLL.COUNT**

    HLL.COUNT key [key ...]

Returns the estimated cardinality (number of unique elements) of the HyperLogLog at `key`.

With a single key, returns the cardinality of that HLL. With multiple keys, returns the cardinality of the union of all specified HLLs—useful for counting unique elements across multiple sets without modifying any of them.

If a key doesn't exist, it contributes 0 to the count.

Example:
```
127.0.0.1:6479> HLL.ADD monday user:1 user:2 user:3
(integer) 1
127.0.0.1:6479> HLL.ADD tuesday user:2 user:3 user:4
(integer) 1
127.0.0.1:6479> HLL.COUNT monday
(integer) 3
127.0.0.1:6479> HLL.COUNT tuesday
(integer) 3
127.0.0.1:6479> HLL.COUNT monday tuesday
(integer) 4
```

**HLL.MERGE**

    HLL.MERGE destkey sourcekey [sourcekey ...]

Merges multiple HyperLogLogs into `destkey`. The resulting HLL represents the union of all source HLLs. If `destkey` exists, it is included in the merge.

This is useful for aggregating counts. For example, merge daily HLLs into a weekly HLL.

Returns `OK`.

Example:
```
127.0.0.1:6479> HLL.MERGE week:1 monday tuesday wednesday
OK
127.0.0.1:6479> HLL.COUNT week:1
(integer) 4
```

---

### Bloom Filter commands

Bloom filters answer "is this element in the set?" with either "definitely no" or "probably yes." They never have false negatives but may have false positives at the configured error rate.

**BF.ADD**

    BF.ADD key item

Adds `item` to the Bloom filter at `key`. Creates the filter if it doesn't exist.

Returns `1` if the item was newly added (wasn't in the filter before), `0` if it was probably already present.

Example:
```
127.0.0.1:6479> BF.ADD emails user@example.com
(integer) 1
127.0.0.1:6479> BF.ADD emails user@example.com
(integer) 0
```

**BF.MADD**

    BF.MADD key item [item ...]

Adds multiple items to the Bloom filter in a single operation. Creates the filter if it doesn't exist.

Returns an array of integers, one per item: `1` if newly added, `0` if probably already present.

Example:
```
127.0.0.1:6479> BF.MADD emails a@test.com b@test.com c@test.com
1) (integer) 1
2) (integer) 1
3) (integer) 1
```

**BF.EXISTS**

    BF.EXISTS key item

Tests whether `item` exists in the Bloom filter.

Returns `1` if the item is probably in the filter (subject to false positive rate), `0` if the item is definitely not in the filter. If the key doesn't exist, returns `0`.

Example:
```
127.0.0.1:6479> BF.EXISTS emails user@example.com
(integer) 1
127.0.0.1:6479> BF.EXISTS emails nobody@example.com
(integer) 0
```

**BF.MEXISTS**

    BF.MEXISTS key item [item ...]

Tests multiple items for existence.

Returns an array of integers, one per item: `1` if probably present, `0` if definitely absent.

---

### Count-Min Sketch commands

Count-Min Sketch estimates the frequency of elements in a data stream. Unlike exact counting, it uses sub-linear space but may over-estimate counts (never under-estimate).

**CMS.INIT**

    CMS.INIT key width depth

Creates a Count-Min Sketch with the specified dimensions. `width` is the number of counters per row, `depth` is the number of rows. More counters = more accuracy but more memory.

Returns an error if the key already exists.

Rule of thumb: `width = ceil(e / epsilon)` where epsilon is your desired error rate, `depth = ceil(ln(1 / delta))` where delta is the probability of exceeding that error.

Example:
```
127.0.0.1:6479> CMS.INIT frequencies 1000 5
OK
```

**CMS.INITBYPROB**

    CMS.INITBYPROB key epsilon delta

Creates a Count-Min Sketch with dimensions automatically calculated from error parameters.

- `epsilon`: Error bound as a fraction (e.g., 0.001 for 0.1% error relative to total count)
- `delta`: Probability of exceeding the error bound (e.g., 0.01 for 1% chance)

Example:
```
127.0.0.1:6479> CMS.INITBYPROB frequencies 0.001 0.01
OK
```

**CMS.INCRBY**

    CMS.INCRBY key item increment [item increment ...]

Increments the count of one or more items. The sketch must already exist.

Uses Conservative Update: instead of blindly incrementing all hash positions, it only raises counters to the new minimum. This significantly reduces over-counting from hash collisions.

Returns an array of the estimated counts after increment.

Example:
```
127.0.0.1:6479> CMS.INCRBY frequencies apple 5 banana 3
1) (integer) 5
2) (integer) 3
127.0.0.1:6479> CMS.INCRBY frequencies apple 2
1) (integer) 7
```

**CMS.QUERY**

    CMS.QUERY key item [item ...]

Returns the estimated count for one or more items.

If the key doesn't exist, returns `0` for all items. Counts are always >= the true count (may over-estimate due to hash collisions).

Example:
```
127.0.0.1:6479> CMS.QUERY frequencies apple banana cherry
1) (integer) 7
2) (integer) 3
3) (integer) 0
```

---

### Top-K commands

Top-K tracks the K most frequent items in a stream using the HeavyKeeper algorithm. Unlike Count-Min Sketch (which tracks all items), Top-K focuses on finding the "heavy hitters."

**TOPK.RESERVE**

    TOPK.RESERVE key k [width depth decay]

Creates a new Top-K structure that tracks the `k` most frequent items.

Optional parameters:
- `width`: Number of buckets per row (default 2048)
- `depth`: Number of rows (default 5)
- `decay`: Probability decay factor between 0 and 1 (default 0.9)

Returns an error if the key already exists.

Example:
```
127.0.0.1:6479> TOPK.RESERVE trending 10
OK
127.0.0.1:6479> TOPK.RESERVE detailed 100 4096 7 0.925
OK
```

**TOPK.ADD**

    TOPK.ADD key item [item ...]

Adds items to the Top-K sketch. Creates the key with default configuration (K=50) if it doesn't exist.

Returns an array with one entry per item:
- `nil` if the item entered or remained in the top-K
- The expelled item's key (as a string) if adding this item caused another item to be kicked out

Example:
```
127.0.0.1:6479> TOPK.ADD trending news:123 news:456 news:123
1) (nil)
2) (nil)
3) (nil)
```

**TOPK.INCRBY**

    TOPK.INCRBY key item increment [item increment ...]

Adds items with specified counts (weighted addition). Creates the key with default configuration (K=50) if it doesn't exist.

Returns the same format as TOPK.ADD.

Example:
```
127.0.0.1:6479> TOPK.INCRBY trending news:789 100
1) (nil)
```

**TOPK.QUERY**

    TOPK.QUERY key item [item ...]

Checks whether items are currently in the top-K.

Returns an array of integers: `1` if the item is in the top-K, `0` otherwise.

Example:
```
127.0.0.1:6479> TOPK.QUERY trending news:123 news:999
1) (integer) 1
2) (integer) 0
```

**TOPK.LIST**

    TOPK.LIST key [WITHCOUNT]

Returns all items currently in the top-K, sorted by count (highest first).

With `WITHCOUNT`, returns alternating item/count pairs.

Example:
```
127.0.0.1:6479> TOPK.LIST trending
1) "news:789"
2) "news:123"
3) "news:456"

127.0.0.1:6479> TOPK.LIST trending WITHCOUNT
1) "news:789"
2) (integer) 100
3) "news:123"
4) (integer) 2
5) "news:456"
6) (integer) 1
```

Persistence
---

Limite uses a hybrid persistence model combining a binary snapshot with an append-only file (AOF) for durability.

**Binary snapshot** (LIM1 format): A compact point-in-time image of all data structures. Each structure is serialized with a magic header for type identification. The snapshot includes a CRC64 checksum for integrity verification.

**Append-only file**: After the snapshot, all write commands are appended as RESP text. This provides durability between snapshots—if the server crashes, it replays these commands on startup.

**Write durability**: The AOF is fsynced to disk every second. This means at most one second of writes can be lost in a crash. If you need stronger guarantees, Limite isn't the right tool—probabilistic data structures are inherently approximate, so losing a second of data rarely matters.

**Automatic compaction**: When the AOF grows beyond a threshold (default: doubles in size from the base), Limite rewrites it as a fresh snapshot. This keeps disk usage bounded and startup time fast.

**Manual compaction**: Use the `COMPACT` command to trigger compaction immediately. This is useful after bulk deletes.

**Graceful shutdown**: On SIGINT/SIGTERM, Limite compacts the AOF before exiting, ensuring the fastest possible startup next time.

Concurrency
---

Limite uses a sharded store with 256 independent shards, each with its own lock. This allows high concurrency—writes to different keys proceed in parallel without contention.

Within each shard:
- Read operations (EXISTS, QUERY, COUNT) acquire a shared lock
- Write operations (ADD, SET) acquire an exclusive lock
- Some operations use optimistic reads with fallback to exclusive locks (HLL.COUNT checks a cache bit first)

Connection handling uses a semaphore to limit concurrent connections (default 100), preventing resource exhaustion under load.

Diagnostic tool
---

Limite includes `limite-check`, a utility for inspecting and validating journal files:

```
$ go build -o limite-check ./cmd/limite-check
$ ./limite-check -file journal.aof
```

The tool validates the binary snapshot structure, verifies the CRC64 checksum, and reports any corruption. With `-v`, it shows per-shard statistics. With `-dump`, it displays the contents of each key.

Use cases
---

**Real-time analytics**: Track unique visitors, page views, or events across millions of users with constant memory. Use HyperLogLog for cardinality, Count-Min Sketch for frequencies.

**Deduplication**: Check if you've processed a message, URL, or event before. Bloom filters give you sub-millisecond lookups with a controllable false positive rate.

**Spam/fraud detection**: Track IP addresses, email domains, or user agents that appear suspiciously often. Top-K identifies heavy hitters without storing every item.

**A/B testing**: Count unique users per experiment variant. Merge daily HLLs into weekly rollups.

**Rate limiting**: Approximate request counts per client. Count-Min Sketch handles millions of clients in megabytes of memory.

**Content recommendation**: Track trending topics or frequently accessed content with Top-K.

Limitations
---

Probabilistic data structures are not a silver bullet:

- **No exact answers**: If you need exact counts or membership, use a traditional database.
- **No element enumeration**: You can't list what's in a Bloom filter or HyperLogLog.
- **Over-estimation bias**: Count-Min Sketch and Bloom filters can say "yes" when the answer is "no" (false positives), but never the reverse.
- **Memory vs accuracy tradeoff**: More memory = more accuracy, but you can't eliminate error entirely.

Limite is designed for use cases where approximate answers are acceptable and memory/performance constraints make exact answers impractical.

Acknowledgments
---

Limite owes a significant debt to Redis and its creator, Salvatore Sanfilippo (antirez). The RESP protocol, command naming conventions, and overall design philosophy are directly inspired by Redis. The code commenting style follows antirez's philosophy on [writing comments to lower cognitive load](https://antirez.com/news/124)—explaining the "why" behind decisions, not just the "what." 

License
---

Limite is open source software. See the LICENSE file for details.

---

*Limite is not affiliated with Redis Ltd. Redis is a trademark of Redis Ltd.*
