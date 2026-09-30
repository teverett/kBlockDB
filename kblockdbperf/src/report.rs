//! Formats a run's results as either a plain-text table (the default) or
//! JSON (`--json`, for piping into something else).

use crate::scenarios::ScenarioResult;

pub fn print_table(results: &[ScenarioResult]) {
    println!(
        "{:<16} {:<42} {:>10} {:>8} {:>9} {:>9} {:>9} {:>9} {:>7}",
        "scenario", "detail", "ops/sec", "ops", "mean_ms", "p50_ms", "p90_ms", "p99_ms", "errors"
    );
    println!("{}", "-".repeat(124));
    for r in results {
        println!(
            "{:<16} {:<42} {:>10.1} {:>8} {:>9.3} {:>9.3} {:>9.3} {:>9.3} {:>7}",
            r.name,
            r.detail,
            r.throughput.ops_per_sec,
            r.throughput.ops,
            r.latency.mean_ms,
            r.latency.p50_ms,
            r.latency.p90_ms,
            r.latency.p99_ms,
            r.latency.errors,
        );
    }
}

pub fn print_json(results: &[ScenarioResult]) {
    match serde_json::to_string_pretty(results) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!("failed to serialize results: {e}"),
    }
}
