use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};

/// Axes stored inline (no heap allocation) in a `Coord`. This prototype
/// targets worlds with a handful of axes; a world with more than this still
/// works, it just heap-allocates per coordinate the way a plain `Vec<u32>`
/// always would have.
const INLINE_AXES: usize = 8;

/// A point in a world's coordinate space: one `u32` per axis, stored inline
/// for up to `INLINE_AXES` axes and spilled to the heap only beyond that.
///
/// This exists because once a world's axis count is a runtime value fixed
/// at `World::create` time (rather than a compile-time constant), a
/// coordinate can't be a fixed-size `[u32; AXES]` array -- but a plain
/// `Vec<u32>` heap-allocates on every single coordinate built (every
/// `get`/`set`/`remove` call, every step of a region iteration...), which
/// measurably slowed things down. This is a small hand-rolled stand-in for
/// what a `smallvec` crate would give you, consistent with the project's
/// zero-dependency stance.
///
/// `Deref`/`DerefMut` to `[u32]` mean it behaves like a slice everywhere
/// else in the crate: indexing, slicing, `.iter()`, `.len()`, and so on all
/// just work without `Coord` needing to reimplement them.
#[derive(Clone)]
pub enum Coord {
    Inline { buf: [u32; INLINE_AXES], len: u8 },
    Heap(Vec<u32>),
}

impl Coord {
    /// `len` zeroed coordinates.
    pub fn zeros(len: usize) -> Coord {
        if len <= INLINE_AXES {
            Coord::Inline {
                buf: [0; INLINE_AXES],
                len: len as u8,
            }
        } else {
            Coord::Heap(vec![0; len])
        }
    }

    fn as_slice(&self) -> &[u32] {
        match self {
            Coord::Inline { buf, len } => &buf[..*len as usize],
            Coord::Heap(v) => v,
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u32] {
        match self {
            Coord::Inline { buf, len } => &mut buf[..*len as usize],
            Coord::Heap(v) => v,
        }
    }
}

impl Deref for Coord {
    type Target = [u32];

    fn deref(&self) -> &[u32] {
        self.as_slice()
    }
}

impl DerefMut for Coord {
    fn deref_mut(&mut self) -> &mut [u32] {
        self.as_mut_slice()
    }
}

impl From<&[u32]> for Coord {
    fn from(s: &[u32]) -> Coord {
        if s.len() <= INLINE_AXES {
            let mut buf = [0u32; INLINE_AXES];
            buf[..s.len()].copy_from_slice(s);
            Coord::Inline {
                buf,
                len: s.len() as u8,
            }
        } else {
            Coord::Heap(s.to_vec())
        }
    }
}

impl From<Vec<u32>> for Coord {
    fn from(v: Vec<u32>) -> Coord {
        // Already-heap-allocated input: for the >INLINE_AXES case, reuse
        // its allocation instead of copying into a fresh one.
        if v.len() <= INLINE_AXES {
            Coord::from(v.as_slice())
        } else {
            Coord::Heap(v)
        }
    }
}

impl<const N: usize> From<[u32; N]> for Coord {
    fn from(a: [u32; N]) -> Coord {
        Coord::from(a.as_slice())
    }
}

impl FromIterator<u32> for Coord {
    fn from_iter<I: IntoIterator<Item = u32>>(iter: I) -> Coord {
        let mut buf = [0u32; INLINE_AXES];
        let mut len = 0usize;
        let mut iter = iter.into_iter();
        for x in iter.by_ref() {
            if len == INLINE_AXES {
                // Overflowed inline capacity: migrate what we have plus the
                // rest of the iterator onto the heap.
                let mut v: Vec<u32> = buf.to_vec();
                v.push(x);
                v.extend(iter);
                return Coord::Heap(v);
            }
            buf[len] = x;
            len += 1;
        }
        Coord::Inline {
            buf,
            len: len as u8,
        }
    }
}

impl PartialEq for Coord {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for Coord {}

impl Hash for Coord {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash as a slice, so a Coord's hash/equality never depends on
        // whether it happens to be Inline or Heap -- only its content, the
        // same way Vec<u32>'s Hash (which this replaces) worked.
        self.as_slice().hash(state);
    }
}

impl fmt::Debug for Coord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_slice(), f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zeros_builds_the_requested_length() {
        assert_eq!(&*Coord::zeros(3), &[0, 0, 0]);
        assert_eq!(Coord::zeros(3).len(), 3);
        assert_eq!(Coord::zeros(0).len(), 0);
    }

    #[test]
    fn zeros_beyond_inline_capacity_still_works() {
        let c = Coord::zeros(INLINE_AXES + 5);
        assert_eq!(c.len(), INLINE_AXES + 5);
        assert!(c.iter().all(|&x| x == 0));
        assert!(matches!(c, Coord::Heap(_)));
    }

    #[test]
    fn small_coords_stay_inline() {
        let c = Coord::from([1u32, 2, 3]);
        assert!(matches!(c, Coord::Inline { .. }));
    }

    #[test]
    fn from_slice_and_from_array_agree() {
        let from_slice = Coord::from([1u32, 2, 3].as_slice());
        let from_array = Coord::from([1u32, 2, 3]);
        assert_eq!(from_slice, from_array);
    }

    #[test]
    fn from_vec_roundtrips_values() {
        let c = Coord::from(vec![9u32, 8, 7, 6]);
        assert_eq!(&*c, &[9, 8, 7, 6]);
    }

    #[test]
    fn collect_from_iterator_builds_a_coord() {
        let c: Coord = (0..5u32).map(|x| x * 10).collect();
        assert_eq!(&*c, &[0, 10, 20, 30, 40]);
    }

    #[test]
    fn collect_beyond_inline_capacity_spills_to_heap() {
        let n = INLINE_AXES + 3;
        let c: Coord = (0..n as u32).collect();
        assert_eq!(c.len(), n);
        assert!(matches!(c, Coord::Heap(_)));
        for (i, &x) in c.iter().enumerate() {
            assert_eq!(x, i as u32);
        }
    }

    #[test]
    fn indexing_and_mutation_work_like_a_slice() {
        let mut c = Coord::from([1u32, 2, 3]);
        assert_eq!(c[1], 2);
        c[1] = 99;
        assert_eq!(c[1], 99);
        for x in c.iter_mut() {
            *x += 1;
        }
        assert_eq!(&*c, &[2, 100, 4]);
    }

    #[test]
    fn equality_and_hash_are_content_based_not_representation_based() {
        use std::collections::hash_map::DefaultHasher;

        let inline = Coord::from([1u32, 2, 3]);
        let heap = Coord::Heap(vec![1, 2, 3]);
        assert_eq!(inline, heap);

        let hash_of = |c: &Coord| {
            let mut h = DefaultHasher::new();
            c.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash_of(&inline), hash_of(&heap));
    }

    #[test]
    fn debug_format_matches_a_plain_slice() {
        let c = Coord::from([1u32, 2, 3]);
        assert_eq!(format!("{c:?}"), format!("{:?}", [1u32, 2, 3]));
    }

    #[test]
    fn clone_is_independent_of_the_original() {
        let mut a = Coord::from([1u32, 2, 3]);
        let b = a.clone();
        a[0] = 99;
        assert_eq!(&*b, &[1, 2, 3]);
    }

    #[test]
    fn usable_as_a_hashmap_key() {
        use std::collections::HashMap;
        let mut m: HashMap<Coord, &str> = HashMap::new();
        m.insert(Coord::from([1u32, 2]), "a");
        assert_eq!(m.get(&Coord::from([1u32, 2])), Some(&"a"));
        assert_eq!(m.get(&Coord::from([1u32, 3])), None);
    }
}
