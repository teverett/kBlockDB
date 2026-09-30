//! Parses the comma-separated coordinate path segments (`/cells/1,2,3/...`,
//! `/regions/0,0,0/8,8,8/...`) into the `Vec<i32>` that `kblockdblib::Region::new`
//! and `World::get`/`set`/`remove` expect. A leading `-` on a component is a
//! plain, valid path character (RFC 3986 doesn't reserve it), so a negative
//! coordinate needs no special URL encoding -- `/cells/-1,2,3/material`
//! works exactly like a non-negative one.

use crate::error::ApiError;

/// Parses `"1,2,3"` into `[1, 2, 3]` (or `"-1,2,3"` into `[-1, 2, 3]`).
/// Rejects an empty string (rather than the empty axis list a naive
/// `split(',')` on `""` would silently produce) and any non-numeric
/// component, both as `400 Bad Request` rather than `500` -- these are
/// caller mistakes, not server failures.
pub fn parse_coords(s: &str) -> Result<Vec<i32>, ApiError> {
    if s.is_empty() {
        return Err(ApiError::BadRequest(
            "coordinate list must not be empty".to_string(),
        ));
    }
    s.split(',')
        .map(|part| {
            part.trim()
                .parse::<i32>()
                .map_err(|_| ApiError::BadRequest(format!("invalid coordinate '{part}'")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_comma_separated_list() {
        assert_eq!(parse_coords("1,2,3").unwrap(), vec![1, 2, 3]);
        assert_eq!(parse_coords("0").unwrap(), vec![0]);
        assert_eq!(parse_coords(" 1 , 2 ").unwrap(), vec![1, 2]);
    }

    #[test]
    fn rejects_empty_string() {
        assert!(matches!(
            parse_coords("").unwrap_err(),
            ApiError::BadRequest(_)
        ));
    }

    #[test]
    fn rejects_non_numeric_components() {
        assert!(matches!(
            parse_coords("1,x,3").unwrap_err(),
            ApiError::BadRequest(_)
        ));
        assert!(matches!(
            parse_coords("1,2.5,3").unwrap_err(),
            ApiError::BadRequest(_)
        ));
    }

    #[test]
    fn accepts_negative_components() {
        assert_eq!(parse_coords("1,-1,3").unwrap(), vec![1, -1, 3]);
        assert_eq!(parse_coords("-5,-5,-5").unwrap(), vec![-5, -5, -5]);
    }
}
