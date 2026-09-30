//! Latency and throughput measurement. Percentiles are computed by sorting
//! a plain `Vec<Duration>` and indexing into it rather than by pulling in a
//! histogram crate -- sample counts here (thousands, not millions) make
//! that both simple and exactly correct.

use std::time::Duration;

#[derive(Debug, Clone, serde::Serialize)]
pub struct LatencyStats {
    /// Successful, timed samples.
    pub count: usize,
    /// Calls that didn't come back with a 2xx status. Timed anyway (so a
    /// scenario dominated by errors still reports believable latency) but
    /// kept out of `count`/the percentiles below, since a fast error isn't
    /// the same thing as a fast success.
    pub errors: usize,
    pub mean_ms: f64,
    pub min_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

impl LatencyStats {
    pub fn from_samples(mut samples: Vec<Duration>, errors: usize) -> LatencyStats {
        if samples.is_empty() {
            return LatencyStats {
                count: 0,
                errors,
                mean_ms: 0.0,
                min_ms: 0.0,
                p50_ms: 0.0,
                p90_ms: 0.0,
                p99_ms: 0.0,
                max_ms: 0.0,
            };
        }
        samples.sort_unstable();
        let count = samples.len();
        let as_ms = |d: Duration| d.as_secs_f64() * 1000.0;
        let sum_ms: f64 = samples.iter().copied().map(as_ms).sum();
        let percentile = |p: f64| -> f64 {
            // Nearest-rank on a 0-indexed sorted array: p=100 must land on
            // the last element exactly, not one past it.
            let idx = ((p / 100.0) * (count - 1) as f64).round() as usize;
            as_ms(samples[idx.min(count - 1)])
        };
        LatencyStats {
            count,
            errors,
            mean_ms: sum_ms / count as f64,
            min_ms: as_ms(samples[0]),
            p50_ms: percentile(50.0),
            p90_ms: percentile(90.0),
            p99_ms: percentile(99.0),
            max_ms: as_ms(samples[count - 1]),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Throughput {
    pub ops: usize,
    pub elapsed_secs: f64,
    pub ops_per_sec: f64,
}

impl Throughput {
    pub fn new(ops: usize, elapsed: Duration) -> Throughput {
        let elapsed_secs = elapsed.as_secs_f64();
        Throughput {
            ops,
            elapsed_secs,
            ops_per_sec: if elapsed_secs > 0.0 {
                ops as f64 / elapsed_secs
            } else {
                0.0
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn stats_of_no_samples_are_all_zero_but_dont_panic() {
        let s = LatencyStats::from_samples(vec![], 3);
        assert_eq!(s.count, 0);
        assert_eq!(s.errors, 3);
        assert_eq!(s.mean_ms, 0.0);
        assert_eq!(s.max_ms, 0.0);
    }

    #[test]
    fn stats_of_one_sample_are_that_sample_everywhere() {
        let s = LatencyStats::from_samples(vec![ms(7)], 0);
        assert_eq!(s.count, 1);
        assert_eq!(s.mean_ms, 7.0);
        assert_eq!(s.min_ms, 7.0);
        assert_eq!(s.p50_ms, 7.0);
        assert_eq!(s.p99_ms, 7.0);
        assert_eq!(s.max_ms, 7.0);
    }

    #[test]
    fn percentiles_on_a_known_sorted_set() {
        // 1..=101 ms, unsorted on input -- from_samples must sort itself.
        let samples: Vec<Duration> = (1..=101).rev().map(ms).collect();
        let s = LatencyStats::from_samples(samples, 0);
        assert_eq!(s.count, 101);
        assert_eq!(s.min_ms, 1.0);
        assert_eq!(s.max_ms, 101.0);
        // Nearest-rank over indices 0..=100: p50 -> index 50 -> value 51.
        assert_eq!(s.p50_ms, 51.0);
        // p100 (used internally as the max bound) must land on the last
        // element, not run off the end.
        assert_eq!(s.p99_ms, ((99.0_f64 / 100.0) * 100.0).round() + 1.0);
    }

    #[test]
    fn errors_dont_count_toward_the_percentiles() {
        let s = LatencyStats::from_samples(vec![ms(1), ms(2), ms(3)], 5);
        assert_eq!(s.count, 3);
        assert_eq!(s.errors, 5);
    }

    #[test]
    fn throughput_computes_ops_per_second() {
        let t = Throughput::new(200, Duration::from_millis(500));
        assert_eq!(t.ops, 200);
        assert!((t.ops_per_sec - 400.0).abs() < 1e-9);
    }

    #[test]
    fn throughput_of_zero_elapsed_is_zero_not_infinity_or_nan() {
        let t = Throughput::new(10, Duration::ZERO);
        assert_eq!(t.ops_per_sec, 0.0);
    }
}
