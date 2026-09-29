use kdb::{chunk, logger, world, Coord, Value, World};
use std::io;
use std::path::Path;
use std::time::Instant;

fn main() -> io::Result<()> {
    let result = run();
    if let Err(e) = &result {
        logger::error(format!("fatal: {e}"));
    }
    result
}

/// A distinct, large, odd-ish multiplier per axis, so scattering an index
/// `i` through every axis (`i.wrapping_mul(axis_multiplier(a)) %
/// world_dim`) spreads points across the whole volume instead of
/// clustering, no matter how many axes the world has.
fn axis_multiplier(axis: usize) -> u32 {
    const HAND_PICKED: [u32; 8] = [
        4_001, 7_919, 104_729, 15_485_863, 32_452_843, 49_979_687, 67_867_967, 86_028_121,
    ];
    HAND_PICKED
        .get(axis)
        .copied()
        .unwrap_or_else(|| 1_000_003u32.wrapping_mul(axis as u32 + 1) | 1)
}

/// `base^axes` as a string: an exact integer when it fits in a `u128`,
/// scientific notation otherwise (`world_dim^axes` overflows a `u128` well
/// before `axes` gets exotic -- e.g. 10,000^10 already doesn't fit).
fn pow_axes_display(base: u32, axes: usize) -> String {
    match (base as u128).checked_pow(axes as u32) {
        Some(n) => n.to_string(),
        None => format!("{:.3e}", f64::from(base).powi(axes as i32)),
    }
}

fn run() -> io::Result<()> {
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./data".to_string());

    logger::info(format!(
        "kdb starting -- data dir: {root} (logging to {})",
        logger::LOG_FILE_NAME
    ));

    // World::create both initializes a brand-new world (first run against
    // this data dir) and validates an existing one (every later run): if
    // `root/world.txt` already holds different axes/world_dim, this fails
    // loudly instead of silently reinterpreting whatever's on disk.
    let mut w = World::create(&root, world::AXES, world::WORLD_DIM)?;
    let axes = w.axes();
    let world_dim = w.world_dim();

    println!("kdb prototype -- chunked columnar key/value cell storage\n");
    println!(
        "world:  {0}^{1} cells  ({2} cells total)",
        world_dim,
        axes,
        pow_axes_display(world_dim, axes)
    );
    println!(
        "chunk:  {0}^{1} cells  ({2} cells/chunk)",
        chunk::CHUNK_DIM,
        axes,
        chunk::chunk_cells(axes)
    );
    println!(
        "grid:   {0}^{1} chunks ({2} chunks total, if every one were populated)",
        w.chunks_per_axis(),
        axes,
        pow_axes_display(w.chunks_per_axis(), axes)
    );
    println!("data dir: {root}\n");

    // --- write a scattered set of cells, the way a sparse simulation would ---
    // A distinct large multiplier per axis (see axis_multiplier) spreads
    // these across the whole world_dim^axes volume instead of clustering in
    // one corner / one chunk.
    let t0 = Instant::now();
    let n = 5_000u32;
    for i in 0..n {
        let coord: Coord = (0..axes)
            .map(|a| i.wrapping_mul(axis_multiplier(a)) % world_dim)
            .collect();

        w.set(
            &coord,
            "material",
            Value::Str(if i % 3 == 0 {
                "stone".into()
            } else {
                "air".into()
            }),
        )?;
        w.set(
            &coord,
            "temperature",
            Value::F64(15.0 + f64::from(i) * 0.01),
        )?;
        w.set(&coord, "density", Value::F64(2.6))?;

        if i % 50 == 0 {
            // A rare, cell-specific key. Because columns are created lazily
            // per chunk, this doesn't cost anything in chunks that never see
            // a "label" key.
            w.set(&coord, "label", Value::Str(format!("poi-{i}")))?;
        }
    }
    w.flush()?;
    let write_elapsed = t0.elapsed();

    println!("wrote {n} cells (4 keys each, 1-in-50 also get a 5th) in {write_elapsed:?}");
    println!(
        "chunk file writes: {}  |  distinct keys interned: {}",
        w.chunks_written_to_disk,
        w.schema_len()
    );
    logger::info(format!(
        "wrote {n} cells in {write_elapsed:?} ({} chunk file writes, {} keys interned)",
        w.chunks_written_to_disk,
        w.schema_len()
    ));

    // A couple of hand-picked cells to demonstrate reads, including a miss.
    let origin: Coord = Coord::zeros(axes);
    w.set(&origin, "material", Value::Str("bedrock".into()))?;
    w.set(&origin, "hardness", Value::I64(10))?;
    w.flush()?;

    // The i=1 case of the scatter loop above.
    let sample: Coord = (0..axes).map(|a| axis_multiplier(a) % world_dim).collect();

    // Demonstrate removal: this cell had a "temperature" key set in the loop
    // above; clear it so the sample read below shows it as <not set>.
    w.remove(&sample, "temperature")?;
    w.flush()?;

    println!("\nsample reads:");
    for coord in [&origin, &sample] {
        println!("  cell {coord:?}:");
        for key in [
            "material",
            "temperature",
            "density",
            "hardness",
            "label",
            "nonexistent_key",
        ] {
            match w.get(coord, key)? {
                Some(v) => println!("    {key:<16} = {v:?}"),
                None => println!("    {key:<16} = <not set>"),
            }
        }
    }

    // --- demonstrate region operations, spanning a chunk boundary ---
    // Starts 3 cells before a chunk boundary and runs 8 cells wide on every
    // axis, so this box covers the tail of one chunk and the head of the
    // next on every axis -- get/set/remove_region resolve each cell through
    // the same chunk/local-index split as the single-cell ops above, so
    // there's nothing extra to do at the seam.
    // set_region takes one value per cell (not a single fill value), so a
    // vein can vary cell-to-cell in one call: every 4th cell is "ore", the
    // rest are "stone".
    let r = chunk::CHUNK_DIM - 3;
    let d = 8;
    let region = world::Region::new(vec![r; axes], vec![d; axes]);
    let vein_values: Vec<Value> = (0..region.volume())
        .map(|i| {
            if i % 4 == 0 {
                Value::Str("ore".into())
            } else {
                Value::Str("stone".into())
            }
        })
        .collect();
    w.set_region(&region, "material", &vein_values)?;
    let vein = w.get_region(&region, "material")?;
    let ore_cells = vein
        .iter()
        .filter(|v| **v == Some(Value::Str("ore".into())))
        .count();
    w.remove_region(&region, "material")?;
    let after_removal = w.get_region(&region, "material")?;
    let remaining = after_removal.iter().filter(|v| v.is_some()).count();
    w.flush()?;

    println!(
        "\nregion ops: filled a {d}^{axes} \"ore\" vein straddling a chunk boundary at \
         {:?} -- {ore_cells}/{} cells set, {remaining}/{} left after remove_region",
        region.origin,
        region.volume(),
        region.volume()
    );

    // --- report the actual on-disk footprint ---
    let (file_count, total_bytes) = disk_usage(Path::new(&root))?;
    let naive_json_estimate = u64::from(n) * 120; // ~120 bytes/cell for a hand-written JSON object
    println!(
        "\non-disk footprint: {file_count} files, {total_bytes} bytes ({:.2} KB)",
        total_bytes as f64 / 1024.0
    );
    println!(
        "for comparison, one small JSON object per cell for just these {n} cells \
         would already run to roughly {naive_json_estimate} bytes, before even \
         touching the trillion-cell filesystem-metadata problem of one file per cell"
    );
    logger::info(format!(
        "on-disk footprint: {file_count} files, {total_bytes} bytes"
    ));

    println!(
        "\nre-run this binary again (same data dir) and it will load the existing \
         chunks/schema instead of starting empty."
    );

    // Drop this handle and reopen with World::open (not create): world.txt
    // is what makes that safe -- open reads the world's real shape back
    // from disk rather than assuming any default, so it's guaranteed to
    // match what create wrote above even if world::AXES/WORLD_DIM change
    // in a future build of this binary.
    drop(w);
    let w = World::open(&root)?;
    println!(
        "reopened via World::open: axes={}, world_dim={} (read back from world.txt, \
         not recompiled in)",
        w.axes(),
        w.world_dim()
    );

    logger::info("kdb finished successfully");
    Ok(())
}

fn disk_usage(dir: &Path) -> io::Result<(u64, u64)> {
    let mut files = 0u64;
    let mut bytes = 0u64;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            let (f, b) = disk_usage(&entry.path())?;
            files += f;
            bytes += b;
        } else {
            files += 1;
            bytes += meta.len();
        }
    }
    Ok((files, bytes))
}
