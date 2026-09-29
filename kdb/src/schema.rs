use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Global key-string <-> integer-id registry for the whole world.
///
/// Storing "temperature" as a 4-byte id instead of an 11-byte string in every
/// one of a trillion cells is most of the win in this design: interning turns
/// "give every cell its own hashmap of strings" into "look up one of a small
/// number of columns by a small integer id".
///
/// The mapping is append-only and persisted as a plain `id\tkey` text file
/// (`schema.txt`) at the world root, so ids never move and old chunk files
/// stay valid no matter how many new keys get introduced later.
pub struct Schema {
    path: PathBuf,
    key_to_id: HashMap<String, u32>,
    id_to_key: Vec<String>,
}

impl Schema {
    pub fn open(world_root: &Path) -> io::Result<Self> {
        let path = world_root.join("schema.txt");
        let mut key_to_id = HashMap::new();
        let mut id_to_key = Vec::new();

        if path.exists() {
            let text = fs::read_to_string(&path)?;
            for line in text.lines() {
                if line.is_empty() {
                    continue;
                }
                let (id_str, key) = line
                    .split_once('\t')
                    .expect("corrupt schema.txt: expected 'id<TAB>key' per line");
                let id: u32 = id_str.parse().expect("corrupt schema.txt: non-numeric id");
                assert_eq!(
                    id as usize,
                    id_to_key.len(),
                    "corrupt schema.txt: ids must be dense and in order"
                );
                id_to_key.push(key.to_string());
                key_to_id.insert(key.to_string(), id);
            }
        }

        Ok(Schema {
            path,
            key_to_id,
            id_to_key,
        })
    }

    pub fn id_for_key(&self, key: &str) -> Option<u32> {
        self.key_to_id.get(key).copied()
    }

    pub fn len(&self) -> usize {
        self.id_to_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_key.is_empty()
    }

    /// Look up `key`'s id, interning (and durably persisting) a new one if
    /// this is the first time this world has ever seen it.
    pub fn intern(&mut self, key: &str) -> io::Result<u32> {
        if let Some(&id) = self.key_to_id.get(key) {
            return Ok(id);
        }

        let id = self.id_to_key.len() as u32;
        self.id_to_key.push(key.to_string());
        self.key_to_id.insert(key.to_string(), id);

        // Append-only on disk too: we never rewrite schema.txt, only add to it.
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(f, "{}\t{}", id, key)?;
        Ok(id)
    }
}
