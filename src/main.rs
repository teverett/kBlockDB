mod chunk;
mod schema;
mod value;
mod world;

use std::io;
use std::path::Path;
use std::time::Instant;
use value::Value;
use world::World;

fn main() -> io::Result<()> {
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./data".to_string());

    let total_cells = (world::WORLD_DIM as u128).pow(3);
    let total_chunks = (world::CHUNKS_PER_AXIS as u128).pow(3);

    println!("kdb prototype -- chunked columnar key/value cell storage\n");
    println!(
        "world:  {0}x{0}x{0} cells  ({1} cells total)",
        world::WORLD_DIM,
        total_cells
    );
    println!(
        "chunk:  {0}x{0}x{0} cells  ({1} cells/chunk)",
        chunk::CHUNK_DIM,
        chunk::CHUNK_CELLS
    );
    println!(
        "grid:   {0}x{0}x{0} chunks ({1} chunks total, if every one were populated)",
        world::CHUNKS_PER_AXIS,
        total_chunks
    );
    println!("data dir: {root}\n");

    let mut w = World::open(&root)?;

    // --- write a scattered set of cells, the way a sparse simulation would ---
    // Large odd multipliers spread these across the whole 10,000^3 volume
    // instead of clustering in one corner / one chunk.
    let t0 = Instant::now();
    let n = 5_000u32;
    for i in 0..n {
        let x = (i.wrapping_mul(4_001)) % world::WORLD_DIM;
        let y = (i.wrapping_mul(7_919)) % world::WORLD_DIM;
        let z = (i.wrapping_mul(104_729)) % world::WORLD_DIM;

        w.set(
            x,
            y,
            z,
            "material",
            Value::Str(if i % 3 == 0 {
                "stone".into()
            } else {
                "air".into()
            }),
        )?;
        w.set(
            x,
            y,
            z,
            "temperature",
            Value::F64(15.0 + f64::from(i) * 0.01),
        )?;
        w.set(x, y, z, "density", Value::F64(2.6))?;

        if i % 50 == 0 {
            // A rare, cell-specific key. Because columns are created lazily
            // per chunk, this doesn't cost anything in chunks that never see
            // a "label" key.
            w.set(x, y, z, "label", Value::Str(format!("poi-{i}")))?;
        }
    }
    w.flush()?;
    let write_elapsed = t0.elapsed();

    println!("wrote {n} cells (4 keys each, 1-in-50 also get a 5th) in {write_elapsed:?}");
    println!(
        "chunk files written: {}  |  distinct keys interned: {}",
        w.chunks_written_to_disk,
        w.schema_len()
    );

    // A couple of hand-picked cells to demonstrate reads, including a miss.
    let (x0, y0, z0) = (0, 0, 0);
    w.set(x0, y0, z0, "material", Value::Str("bedrock".into()))?;
    w.set(x0, y0, z0, "hardness", Value::I64(10))?;
    w.flush()?;

    let (x1, y1, z1) = (
        4_001 % world::WORLD_DIM,
        7_919 % world::WORLD_DIM,
        104_729 % world::WORLD_DIM,
    );

    // Demonstrate removal: this cell had a "temperature" key set in the loop
    // above; clear it so the sample read below shows it as <not set>.
    w.remove(x1, y1, z1, "temperature")?;
    w.flush()?;

    println!("\nsample reads:");
    for &(x, y, z) in &[(x0, y0, z0), (x1, y1, z1)] {
        println!("  cell ({x}, {y}, {z}):");
        for key in [
            "material",
            "temperature",
            "density",
            "hardness",
            "label",
            "nonexistent_key",
        ] {
            match w.get(x, y, z, key)? {
                Some(v) => println!("    {key:<16} = {v:?}"),
                None => println!("    {key:<16} = <not set>"),
            }
        }
    }

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

    println!(
        "\nre-run this binary again (same data dir) and it will load the existing \
         chunks/schema instead of starting empty."
    );

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
