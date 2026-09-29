//! The actual benchmark scenarios. Each returns one or more
//! [`ScenarioResult`]s -- one per swept parameter value, for the scenarios
//! that sweep something (region size, concurrency level).
//!
//! Coordinates are generated for whatever axis count the target world
//! actually has (queried via `/health`), the same way `kdb`'s own demo
//! spreads points across a volume -- nothing here is 3-axis-specific.

use crate::client::Client;
use crate::stats::{LatencyStats, Throughput};
use serde::Serialize;
use std::time::Instant;

#[derive(Serialize)]
pub struct ScenarioResult {
    pub name: String,
    pub detail: String,
    pub throughput: Throughput,
    pub latency: LatencyStats,
}

/// A distinct, large, odd-ish multiplier per axis, so spreading a seed
/// through every axis doesn't cluster -- the same trick `kdb`'s own demo
/// (`kdb/src/main.rs`) uses, reimplemented here since kdbperf deliberately
/// treats kdbserver as a black box over HTTP rather than depending on kdb.
fn axis_multiplier(axis: usize) -> u64 {
    const HAND_PICKED: [u64; 8] = [
        4_001, 7_919, 104_729, 15_485_863, 32_452_843, 49_979_687, 67_867_967, 86_028_121,
    ];
    HAND_PICKED
        .get(axis)
        .copied()
        .unwrap_or_else(|| 1_000_003u64.wrapping_mul(axis as u64 + 1) | 1)
}

/// Spreads `seed` into an `axes`-long coordinate within `[0, world_dim)`.
/// `salt` decorrelates different scenarios'/reps' coordinate sets from
/// each other so they don't all hammer the exact same cells by accident.
fn spread_coord(axes: usize, world_dim: u32, seed: u64, salt: u64) -> Vec<u32> {
    let seed = seed.wrapping_add(salt.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let world_dim = world_dim.max(1) as u64;
    (0..axes)
        .map(|a| (seed.wrapping_mul(axis_multiplier(a)) % world_dim) as u32)
        .collect()
}

async fn aggregate<F, Fut>(
    concurrency: usize,
    ops_per_worker: usize,
    work: F,
) -> (Vec<std::time::Duration>, usize, std::time::Duration)
where
    F: Fn(usize, usize) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = (Vec<std::time::Duration>, usize)> + Send + 'static,
{
    let work = std::sync::Arc::new(work);
    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let work = work.clone();
        handles.push(tokio::spawn(
            async move { work(worker, ops_per_worker).await },
        ));
    }
    let mut samples = Vec::with_capacity(concurrency * ops_per_worker);
    let mut errors = 0;
    for h in handles {
        let (s, e) = h.await.expect("benchmark worker task panicked");
        samples.extend(s);
        errors += e;
    }
    (samples, errors, t0.elapsed())
}

/// Sequential (one client, no concurrency) `set` on `n` distinct cells.
pub async fn set_cell(client: &Client, axes: usize, world_dim: u32, n: usize) -> ScenarioResult {
    let mut samples = Vec::with_capacity(n);
    let mut errors = 0;
    let t0 = Instant::now();
    for i in 0..n {
        let coord = spread_coord(axes, world_dim, i as u64, 1);
        let t = client.set_cell(&coord, "bench", i as i64).await;
        errors += usize::from(!t.ok);
        samples.push(t.elapsed);
    }
    ScenarioResult {
        name: "set_cell".into(),
        detail: format!("n={n}, sequential"),
        throughput: Throughput::new(n, t0.elapsed()),
        latency: LatencyStats::from_samples(samples, errors),
    }
}

/// Sequential `get` on `n` cells this scenario populates itself first
/// (untimed), so it's runnable in isolation and measures only the read.
pub async fn get_cell(client: &Client, axes: usize, world_dim: u32, n: usize) -> ScenarioResult {
    for i in 0..n {
        let coord = spread_coord(axes, world_dim, i as u64, 2);
        client.set_cell(&coord, "bench", i as i64).await;
    }

    let mut samples = Vec::with_capacity(n);
    let mut errors = 0;
    let t0 = Instant::now();
    for i in 0..n {
        let coord = spread_coord(axes, world_dim, i as u64, 2);
        let t = client.get_cell(&coord, "bench").await;
        errors += usize::from(!t.ok);
        samples.push(t.elapsed);
    }
    ScenarioResult {
        name: "get_cell".into(),
        detail: format!("n={n}, sequential"),
        throughput: Throughput::new(n, t0.elapsed()),
        latency: LatencyStats::from_samples(samples, errors),
    }
}

/// Sequential `remove` on `n` cells this scenario populates itself first.
pub async fn remove_cell(client: &Client, axes: usize, world_dim: u32, n: usize) -> ScenarioResult {
    for i in 0..n {
        let coord = spread_coord(axes, world_dim, i as u64, 3);
        client.set_cell(&coord, "bench", i as i64).await;
    }

    let mut samples = Vec::with_capacity(n);
    let mut errors = 0;
    let t0 = Instant::now();
    for i in 0..n {
        let coord = spread_coord(axes, world_dim, i as u64, 3);
        let t = client.remove_cell(&coord, "bench").await;
        errors += usize::from(!t.ok);
        samples.push(t.elapsed);
    }
    ScenarioResult {
        name: "remove_cell".into(),
        detail: format!("n={n}, sequential"),
        throughput: Throughput::new(n, t0.elapsed()),
        latency: LatencyStats::from_samples(samples, errors),
    }
}

/// `set_region`/`get_region` at each edge length in `edges` (a cube of
/// that edge on every axis, e.g. edge=8 with 3 axes is a 512-cell region),
/// `reps` times each, all at the same origin (distinctness doesn't matter
/// for a perf measurement -- each call just overwrites). An `edge` larger
/// than the target's `world_dim` just shows up as an error count in the
/// result, rather than being pre-validated here.
pub async fn region_sweep(
    client: &Client,
    axes: usize,
    edges: &[u32],
    reps: usize,
) -> Vec<ScenarioResult> {
    let mut results = Vec::new();
    let origin = vec![0u32; axes];

    for &edge in edges {
        let extent = vec![edge; axes];
        let volume = extent.iter().map(|&e| u64::from(e)).product::<u64>() as usize;
        let values: Vec<i64> = (0..volume as i64).collect();

        let mut set_samples = Vec::with_capacity(reps);
        let mut set_errors = 0;
        let t0 = Instant::now();
        for _ in 0..reps {
            let t = client.set_region(&origin, &extent, "bench", &values).await;
            set_errors += usize::from(!t.ok);
            set_samples.push(t.elapsed);
        }
        results.push(ScenarioResult {
            name: "set_region".into(),
            detail: format!("edge={edge}, volume={volume}, reps={reps}"),
            throughput: Throughput::new(volume * reps, t0.elapsed()),
            latency: LatencyStats::from_samples(set_samples, set_errors),
        });

        let mut get_samples = Vec::with_capacity(reps);
        let mut get_errors = 0;
        let t0 = Instant::now();
        for _ in 0..reps {
            let t = client.get_region(&origin, &extent, "bench").await;
            get_errors += usize::from(!t.ok);
            get_samples.push(t.elapsed);
        }
        results.push(ScenarioResult {
            name: "get_region".into(),
            detail: format!("edge={edge}, volume={volume}, reps={reps}"),
            throughput: Throughput::new(volume * reps, t0.elapsed()),
            latency: LatencyStats::from_samples(get_samples, get_errors),
        });
    }
    results
}

/// `set` from `concurrency` concurrent clients, each on its own disjoint
/// cells (different chunks, typically), at each concurrency level in
/// `levels`. Shows how throughput scales with concurrency when requests
/// don't contend for the same chunk.
pub async fn concurrency_scan(
    client: &Client,
    axes: usize,
    world_dim: u32,
    levels: &[usize],
    ops_per_client: usize,
) -> Vec<ScenarioResult> {
    let mut results = Vec::new();
    for &level in levels {
        let client = client.clone();
        let (samples, errors, elapsed) = aggregate(level, ops_per_client, move |worker, n| {
            let client = client.clone();
            async move {
                let mut samples = Vec::with_capacity(n);
                let mut errors = 0;
                for op in 0..n {
                    let seed = (worker * n + op) as u64;
                    let coord = spread_coord(axes, world_dim, seed, 10);
                    let t = client.set_cell(&coord, "bench", seed as i64).await;
                    errors += usize::from(!t.ok);
                    samples.push(t.elapsed);
                }
                (samples, errors)
            }
        })
        .await;
        results.push(ScenarioResult {
            name: "concurrency_scan".into(),
            detail: format!("concurrency={level}, ops_per_client={ops_per_client}, disjoint cells"),
            throughput: Throughput::new(level * ops_per_client, elapsed),
            latency: LatencyStats::from_samples(samples, errors),
        });
    }
    results
}

/// `set` from `concurrency` concurrent clients, all on the *same* cell
/// (different keys, so it's not just racing an identical overwrite), at
/// each concurrency level in `levels`. `kdb::World::set` takes an
/// exclusive lock on that cell's chunk file for each call, so unlike
/// `concurrency_scan`, throughput here shouldn't meaningfully improve past
/// whatever one lock holder can push through -- contrasting the two
/// scenarios is the point.
pub async fn contended_cell(
    client: &Client,
    axes: usize,
    levels: &[usize],
    ops_per_client: usize,
) -> Vec<ScenarioResult> {
    let coord: Vec<u32> = vec![1; axes];
    let mut results = Vec::new();
    for &level in levels {
        let client = client.clone();
        let coord = coord.clone();
        let (samples, errors, elapsed) = aggregate(level, ops_per_client, move |worker, n| {
            let client = client.clone();
            let coord = coord.clone();
            async move {
                let key = format!("bench-{worker}");
                let mut samples = Vec::with_capacity(n);
                let mut errors = 0;
                for op in 0..n {
                    let t = client.set_cell(&coord, &key, op as i64).await;
                    errors += usize::from(!t.ok);
                    samples.push(t.elapsed);
                }
                (samples, errors)
            }
        })
        .await;
        results.push(ScenarioResult {
            name: "contended_cell".into(),
            detail: format!("concurrency={level}, ops_per_client={ops_per_client}, same cell"),
            throughput: Throughput::new(level * ops_per_client, elapsed),
            latency: LatencyStats::from_samples(samples, errors),
        });
    }
    results
}

/// `set` from `concurrency` concurrent clients spread evenly across
/// `clients` (round-robin), each on its own disjoint cells. Run once with
/// one server and once with several servers sharing the same data
/// directory at the same total concurrency, this is what actually shows
/// multi-process scaling: a single kdbserver process serializes every
/// request through one `Mutex<World>` regardless of kdb's own per-chunk
/// locking, so that lock only stops mattering once there's more than one
/// process to spread load across.
pub async fn multi_instance(
    clients: &[Client],
    axes: usize,
    world_dim: u32,
    concurrency: usize,
    ops_per_client: usize,
) -> ScenarioResult {
    let clients: Vec<Client> = clients.to_vec();
    let n_instances = clients.len();
    let (samples, errors, elapsed) = aggregate(concurrency, ops_per_client, move |worker, n| {
        let client = clients[worker % clients.len()].clone();
        async move {
            let mut samples = Vec::with_capacity(n);
            let mut errors = 0;
            for op in 0..n {
                let seed = (worker * n + op) as u64;
                let coord = spread_coord(axes, world_dim, seed, 20);
                let t = client.set_cell(&coord, "bench", seed as i64).await;
                errors += usize::from(!t.ok);
                samples.push(t.elapsed);
            }
            (samples, errors)
        }
    })
    .await;
    ScenarioResult {
        name: "multi_instance".into(),
        detail: format!(
            "instances={n_instances}, concurrency={concurrency}, ops_per_client={ops_per_client}"
        ),
        throughput: Throughput::new(concurrency * ops_per_client, elapsed),
        latency: LatencyStats::from_samples(samples, errors),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spread_coord_stays_in_bounds_and_has_the_right_axis_count() {
        for axes in 1..=6 {
            for seed in 0..50u64 {
                let c = spread_coord(axes, 1000, seed, 7);
                assert_eq!(c.len(), axes);
                assert!(c.iter().all(|&x| x < 1000));
            }
        }
    }

    #[test]
    fn spread_coord_is_deterministic() {
        assert_eq!(
            spread_coord(3, 10_000, 42, 5),
            spread_coord(3, 10_000, 42, 5)
        );
    }

    #[test]
    fn different_salt_usually_gives_a_different_coordinate() {
        // Not a mathematical guarantee, but with a 10,000-wide axis a
        // collision across two different salts at the same seed would be
        // a very suspicious coincidence worth knowing about.
        let a = spread_coord(3, 10_000, 42, 1);
        let b = spread_coord(3, 10_000, 42, 2);
        assert_ne!(a, b);
    }

    #[test]
    fn world_dim_of_one_never_divides_by_zero() {
        let c = spread_coord(3, 0, 42, 1); // world_dim.max(1) guards this
        assert_eq!(c, vec![0, 0, 0]);
    }
}
