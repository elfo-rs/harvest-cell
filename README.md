# harvest-cell

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![License][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]
[![Codecov][codecov-badge]][codecov-url]

[crates-badge]: https://img.shields.io/crates/v/harvest-cell.svg
[crates-url]: https://crates.io/crates/harvest-cell
[docs-badge]: https://img.shields.io/docsrs/harvest-cell
[docs-url]: https://docs.rs/harvest-cell
[license-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[license-url]: https://github.com/elfo-rs/harvest-cell/blob/master/LICENSE
[actions-badge]: https://github.com/elfo-rs/harvest-cell/actions/workflows/ci.yml/badge.svg
[actions-url]: https://github.com/elfo-rs/harvest-cell/actions/workflows/ci.yml
[codecov-badge]: https://codecov.io/gh/elfo-rs/harvest-cell/graph/badge.svg
[codecov-url]: https://codecov.io/gh/elfo-rs/harvest-cell

Collect accumulated data across threads without blocking writers.

[API documentation](https://docs.rs/harvest-cell) | [Telemetry example](https://github.com/elfo-rs/harvest-cell/tree/master/examples/telemetry)

## Motivation

`harvest-cell` grew out of the telemetry storage in [elfo-telemeter](https://github.com/elfo-rs/elfo/tree/master/elfo-telemeter).
Actors update counters, gauges and record histogram samples as they process messages.
Each thread accumulates these updates in its own thread-local shard, avoiding contention between threads that update the same metrics.
The telemeter actor periodically collects the shards and merges their data into a snapshot for export.

With one shard per thread and infrequent collection, a mutex around each shard is usually uncontended.
Even an uncontended mutex adds synchronization overhead to every write, particularly on x86-64 (see [benchmarks](#benchmarks)).
We considered holding the shard lock for an entire actor poll to amortize its cost across writes.
However, a stuck poll would block collection for the entire shard, including metrics from all actors that wrote to it.
Another option was to wait for writers to acknowledge buffer switches, but collection would then depend on the next write on that thread.
We need to collect all recorded metrics without waiting indefinitely to respond to `/metrics` requests.

That led us to `harvest-cell`.
`HarvestCell<T>` is a double buffer with a writer side and a harvester side.
The writer updates one buffer while the harvester collects the other.
When starting a new harvest for a cell, `Harvester::start` switches subsequent writes to the other buffer, allowing the writer to continue without waiting for the harvest to finish.
It then applies a single memory barrier, after which at most one write per cell can still access the previous buffer.
The harvest only needs that write to finish.

## Usage

Add `harvest-cell` to your `Cargo.toml`:

```toml
[dependencies]
harvest-cell = "0.1"
```

Create two buffers and split the cell into a writer and a harvest handle:

```rust
use harvest_cell::{HarvestCell, Harvester};

// `into_split` owns the cell, so the writer and the harvest handle can be
// moved to different threads.
let (mut writer, mut handle) =
    HarvestCell::new([Vec::new(), Vec::new()]).into_split();

writer.write(|samples| samples.push(42));

// Switch writes to the other buffer. An iterator of handles switches
// several cells with a single memory barrier.
let mut harvester = Harvester::start(&mut handle);

// Take the samples out of the previous buffer. The writer switches back to
// the emptied buffer on the next `start`.
assert_eq!(harvester.next_with(std::mem::take), Some(vec![42]));
```

## Examples

The [telemetry example](https://github.com/elfo-rs/harvest-cell/tree/master/examples/telemetry) demonstrates a simplified collector that periodically drains latency samples from thread-local `HarvestCell<Vec<u64>>` shards, retaining their capacity for subsequent writes.

## Features

By default, harvest-cell enables no feature flags, and the writer uses sequentially consistent atomics.

- `tokio`: Enables `Harvester::async_next_with` to wait asynchronously for ready buffers.
- `fence_fallback`: Uses an explicit hardware fence instead of sequentially consistent stores and loads on the write path.
- `membarrier`: Uses Linux [`membarrier`](https://man7.org/linux/man-pages/man2/membarrier.2.html) to move the hardware barrier cost from each write to the harvest.
  Falls back to `fence_fallback` on other operating systems and when registration fails.
  The syscall can briefly interrupt other threads in the process, including writers.
- `loom`: Enables the dependency for model checking.
  Model tests also require `--cfg harvest_cell_loom` and `fence_fallback`.

## Benchmarks

The [writer benchmark](https://github.com/elfo-rs/harvest-cell/blob/master/benches/write.rs) measures the cost of one counter update without concurrent harvesting.
We ran it on macOS (`AArch64`) and Linux (`x86-64`) using Rust 1.98.1 release builds and [Criterion 0.8.2](https://github.com/criterion-rs/criterion.rs).
Linux runs use isolated CPU cores with CPU pinning and the performance governor; the macOS run uses no CPU pinning.
Each case uses 100 samples, a five-second warmup, and a ten-second measurement period.

The table reports estimates in nanoseconds per update; lower is better.

<table>
  <thead>
    <tr>
      <th rowspan="2">Implementation</th>
      <th>macOS / AArch64</th>
      <th colspan="4">Linux / x86-64</th>
    </tr>
    <tr>
      <th>Apple<br>M3 Max</th>
      <th>Intel Xeon<br>Gold 6526Y</th>
      <th>AMD Ryzen 9<br>9950X3D</th>
      <th>Intel Core Ultra 9<br>285K</th>
      <th>AMD EPYC<br>9375F</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>std::sync::Mutex</code></td>
      <td align="right">4.635</td>
      <td align="right">12.350</td>
      <td align="right">7.288</td>
      <td align="right">9.073</td>
      <td align="right">8.485</td>
    </tr>
    <tr>
      <td><code>parking_lot::Mutex</code></td>
      <td align="right">2.109</td>
      <td align="right">12.855</td>
      <td align="right">7.099</td>
      <td align="right">9.269</td>
      <td align="right">8.277</td>
    </tr>
    <tr>
      <td><code>harvest-cell</code> (default)</td>
      <td align="right">0.629</td>
      <td align="right">7.429</td>
      <td align="right">3.733</td>
      <td align="right">4.999</td>
      <td align="right">4.346</td>
    </tr>
    <tr>
      <td><code>harvest-cell</code> + <code>fence_fallback</code></td>
      <td align="right">0.888</td>
      <td align="right">7.142</td>
      <td align="right">4.088</td>
      <td align="right">5.184</td>
      <td align="right">4.760</td>
    </tr>
    <tr>
      <td><code>harvest-cell</code> + <code>membarrier</code></td>
      <td align="right">—</td>
      <td align="right">2.104</td>
      <td align="right">0.727</td>
      <td align="right">1.222</td>
      <td align="right">0.828</td>
    </tr>
  </tbody>
</table>

<details>
<summary>Full estimates and 95% confidence intervals</summary>

**Apple M3 Max (macOS / `AArch64`)**

<table>
  <thead>
    <tr><th>Implementation</th><th>Estimate</th><th>95% CI</th></tr>
  </thead>
  <tbody>
    <tr><td><code>std::sync::Mutex</code></td><td align="right">4.635</td><td align="right">[4.582–4.671]</td></tr>
    <tr><td><code>parking_lot::Mutex</code></td><td align="right">2.109</td><td align="right">[2.103–2.115]</td></tr>
    <tr><td><code>harvest-cell</code> (default)</td><td align="right">0.629</td><td align="right">[0.625–0.633]</td></tr>
    <tr><td><code>harvest-cell</code> + <code>fence_fallback</code></td><td align="right">0.888</td><td align="right">[0.876–0.900]</td></tr>
  </tbody>
</table>

**Intel Xeon Gold 6526Y (Linux / x86-64)**

<table>
  <thead>
    <tr><th>Implementation</th><th>Estimate</th><th>95% CI</th></tr>
  </thead>
  <tbody>
    <tr><td><code>std::sync::Mutex</code></td><td align="right">12.350</td><td align="right">[12.349–12.352]</td></tr>
    <tr><td><code>parking_lot::Mutex</code></td><td align="right">12.8551</td><td align="right">[12.8550–12.8552]</td></tr>
    <tr><td><code>harvest-cell</code> (default)</td><td align="right">7.429</td><td align="right">[7.427–7.430]</td></tr>
    <tr><td><code>harvest-cell</code> + <code>fence_fallback</code></td><td align="right">7.14169</td><td align="right">[7.14166–7.14173]</td></tr>
    <tr><td><code>harvest-cell</code> + <code>membarrier</code></td><td align="right">2.104</td><td align="right">[2.103–2.105]</td></tr>
  </tbody>
</table>

**AMD Ryzen 9 9950X3D (Linux / x86-64)**

<table>
  <thead>
    <tr><th>Implementation</th><th>Estimate</th><th>95% CI</th></tr>
  </thead>
  <tbody>
    <tr><td><code>std::sync::Mutex</code></td><td align="right">7.288</td><td align="right">[7.286–7.291]</td></tr>
    <tr><td><code>parking_lot::Mutex</code></td><td align="right">7.099</td><td align="right">[7.098–7.100]</td></tr>
    <tr><td><code>harvest-cell</code> (default)</td><td align="right">3.7328</td><td align="right">[3.7327–3.7330]</td></tr>
    <tr><td><code>harvest-cell</code> + <code>fence_fallback</code></td><td align="right">4.0880</td><td align="right">[4.0878–4.0882]</td></tr>
    <tr><td><code>harvest-cell</code> + <code>membarrier</code></td><td align="right">0.7267</td><td align="right">[0.7267–0.7268]</td></tr>
  </tbody>
</table>

**Intel Core Ultra 9 285K (Linux / x86-64)**

<table>
  <thead>
    <tr><th>Implementation</th><th>Estimate</th><th>95% CI</th></tr>
  </thead>
  <tbody>
    <tr><td><code>std::sync::Mutex</code></td><td align="right">9.0730</td><td align="right">[9.0729–9.0731]</td></tr>
    <tr><td><code>parking_lot::Mutex</code></td><td align="right">9.269</td><td align="right">[9.268–9.269]</td></tr>
    <tr><td><code>harvest-cell</code> (default)</td><td align="right">4.99897</td><td align="right">[4.99897–4.99898]</td></tr>
    <tr><td><code>harvest-cell</code> + <code>fence_fallback</code></td><td align="right">5.18411</td><td align="right">[5.18410–5.18411]</td></tr>
    <tr><td><code>harvest-cell</code> + <code>membarrier</code></td><td align="right">1.2219</td><td align="right">[1.2218–1.2221]</td></tr>
  </tbody>
</table>

**AMD EPYC 9375F (Linux / x86-64)**

<table>
  <thead>
    <tr><th>Implementation</th><th>Estimate</th><th>95% CI</th></tr>
  </thead>
  <tbody>
    <tr><td><code>std::sync::Mutex</code></td><td align="right">8.4852</td><td align="right">[8.4850–8.4854]</td></tr>
    <tr><td><code>parking_lot::Mutex</code></td><td align="right">8.2772</td><td align="right">[8.2770–8.2774]</td></tr>
    <tr><td><code>harvest-cell</code> (default)</td><td align="right">4.3461</td><td align="right">[4.3460–4.3462]</td></tr>
    <tr><td><code>harvest-cell</code> + <code>fence_fallback</code></td><td align="right">4.7598</td><td align="right">[4.7597–4.7599]</td></tr>
    <tr><td><code>harvest-cell</code> + <code>membarrier</code></td><td align="right">0.8279</td><td align="right">[0.8279–0.8280]</td></tr>
  </tbody>
</table>

</details>
