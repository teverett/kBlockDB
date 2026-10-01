//! A minimal, dependency-free logger that appends timestamped lines to
//! `kblockdblib.log` in the current working directory.
//!
//! Kept hand-rolled (no `log`/`env_logger`/`chrono` crate) for the same
//! reason the rest of this project is: `kblockdblib` takes on no
//! dependency it doesn't have to (`zstd`, for the optional `compression`
//! flag, is the single exception). It's a
//! single append-mode file handle behind a mutex, three severity levels,
//! and a UTC timestamp computed from `SystemTime` with a small, well-known
//! days-since-epoch -> calendar-date algorithm.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Log file, created/appended in the current working directory.
pub const LOG_FILE_NAME: &str = "kblockdblib.log";

static LOG_FILE: OnceLock<Mutex<File>> = OnceLock::new();

#[derive(Clone, Copy)]
pub enum Level {
    Info,
    Warn,
    Error,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        }
    }
}

/// Append one line to the process-wide log file (`kblockdblib.log`). Logging
/// failures are swallowed on purpose: a full disk or unwritable directory
/// shouldn't take the program down just because it couldn't log.
pub fn log(level: Level, msg: impl AsRef<str>) {
    let file = LOG_FILE.get_or_init(|| {
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(LOG_FILE_NAME)
            .unwrap_or_else(|e| panic!("failed to open {LOG_FILE_NAME}: {e}"));
        Mutex::new(f)
    });
    if let Ok(mut f) = file.lock() {
        let _ = write_line(&mut *f, level, msg.as_ref());
    }
}

pub fn info(msg: impl AsRef<str>) {
    log(Level::Info, msg);
}

pub fn warn(msg: impl AsRef<str>) {
    log(Level::Warn, msg);
}

pub fn error(msg: impl AsRef<str>) {
    log(Level::Error, msg);
}

/// Writes one formatted line: `<UTC timestamp> [<LEVEL>] <message>\n`.
/// Split out from `log()` so the formatting itself is testable against an
/// in-memory buffer, without touching the real log file or its global lock.
fn write_line<W: Write>(w: &mut W, level: Level, msg: &str) -> io::Result<()> {
    writeln!(w, "{} [{}] {msg}", utc_timestamp(), level.as_str())
}

/// Formats the current time as `YYYY-MM-DD HH:MM:SS UTC`.
fn utc_timestamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let (y, mo, d, h, mi, s) = civil_from_unix(secs as i64);
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02} UTC")
}

/// Converts seconds since the Unix epoch into a UTC
/// (year, month, day, hour, minute, second) tuple.
///
/// Implements Howard Hinnant's `civil_from_days` algorithm
/// (<http://howardhinnant.github.io/date_algorithms.html>), which avoids
/// pulling in a whole date/time crate just to stamp log lines. Correct for
/// the proleptic Gregorian calendar, including dates before 1970.
fn civil_from_unix(unix_secs: i64) -> (i64, u32, u32, u32, u32, u32) {
    let mut days = unix_secs.div_euclid(86_400);
    let time_of_day = unix_secs.rem_euclid(86_400);
    let h = (time_of_day / 3600) as u32;
    let mi = ((time_of_day % 3600) / 60) as u32;
    let s = (time_of_day % 60) as u32;

    // civil_from_days, with `days` shifted so day 0 is 0000-03-01.
    days += 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = (days - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy + 1 - (153 * mp + 2) / 5) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };

    (y, m, d, h, mi, s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_from_unix_epoch_is_1970_01_01() {
        assert_eq!(civil_from_unix(0), (1970, 1, 1, 0, 0, 0));
    }

    #[test]
    fn civil_from_unix_known_date_and_time() {
        // 2024-01-01T13:45:30Z
        assert_eq!(civil_from_unix(1_704_116_730), (2024, 1, 1, 13, 45, 30));
    }

    #[test]
    fn civil_from_unix_handles_leap_day() {
        // 2024-02-29T00:00:00Z (2024 is a leap year)
        assert_eq!(civil_from_unix(1_709_164_800), (2024, 2, 29, 0, 0, 0));
    }

    #[test]
    fn civil_from_unix_handles_pre_epoch_dates() {
        // 1969-12-31T23:59:59Z, one second before the epoch.
        assert_eq!(civil_from_unix(-1), (1969, 12, 31, 23, 59, 59));
    }

    #[test]
    fn write_line_formats_timestamp_level_and_message() {
        let mut buf = Vec::new();
        write_line(&mut buf, Level::Warn, "cache eviction skipped").unwrap();
        let line = String::from_utf8(buf).unwrap();

        assert!(line.ends_with("[WARN] cache eviction skipped\n"));
        let timestamp = line.split(" [").next().unwrap();
        assert!(timestamp.ends_with(" UTC"));
        assert_eq!(timestamp.len(), "2024-01-01 13:45:30 UTC".len());
    }

    #[test]
    fn write_line_uses_each_level_label() {
        for (level, label) in [
            (Level::Info, "INFO"),
            (Level::Warn, "WARN"),
            (Level::Error, "ERROR"),
        ] {
            let mut buf = Vec::new();
            write_line(&mut buf, level, "x").unwrap();
            let line = String::from_utf8(buf).unwrap();
            assert!(line.contains(&format!("[{label}] x")));
        }
    }
}
