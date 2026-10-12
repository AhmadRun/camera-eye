//! Port of `server/providers/cctv/range.js`: the client `Range` header is
//! validated, canonicalized and bounded before it reaches a camera host.

use super::constants::CCTV_MEDIA_MAX_BODY_BYTES;
use super::jsval;

/// Digits allowed per Range position.
const RANGE_MAX_DIGITS: usize = 16;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// `sanitizeCctvRangeHeader`: a canonical single `bytes=` range bounded to
/// `max_bytes`, or `""` to send no Range at all.
pub fn sanitize_cctv_range_header(value: Option<&str>, max_bytes: u64) -> String {
    let Some(value) = value else {
        return String::new();
    };
    let raw = jsval::trim(value);
    if raw.is_empty() {
        return String::new();
    }
    let Some(spec) = raw
        .get(..6)
        .filter(|unit| unit.eq_ignore_ascii_case("bytes="))
        .map(|_| &raw[6..])
    else {
        return String::new();
    };
    let Some((first_text, last_text)) = spec.split_once('-') else {
        return String::new();
    };
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if !digits(first_text) || !digits(last_text) {
        return String::new();
    }
    if first_text.len() > RANGE_MAX_DIGITS || last_text.len() > RANGE_MAX_DIGITS {
        return String::new();
    }
    if first_text.is_empty() && last_text.is_empty() {
        return String::new();
    }
    let parse = |s: &str| s.parse::<u64>().ok().filter(|n| *n <= MAX_SAFE_INTEGER);
    if first_text.is_empty() {
        return match parse(last_text) {
            Some(suffix) if suffix > 0 => format!("bytes=-{}", suffix.min(max_bytes)),
            _ => String::new(),
        };
    }
    let Some(first) = parse(first_text) else {
        return String::new();
    };
    let ceiling = first + max_bytes - 1;
    if ceiling > MAX_SAFE_INTEGER {
        return String::new();
    }
    if last_text.is_empty() {
        return format!("bytes={first}-{ceiling}");
    }
    match parse(last_text) {
        Some(last) if last >= first => format!("bytes={first}-{}", last.min(ceiling)),
        _ => String::new(),
    }
}

/// [`sanitize_cctv_range_header`] with the relay's body cap.
pub fn sanitize(value: Option<&str>) -> String {
    sanitize_cctv_range_header(value, CCTV_MEDIA_MAX_BODY_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAP: u64 = CCTV_MEDIA_MAX_BODY_BYTES;

    #[test]
    fn single_ranges_pass_through_canonicalized() {
        assert_eq!(sanitize(Some("bytes=0-1023")), "bytes=0-1023");
        assert_eq!(sanitize(Some("bytes=7-7")), "bytes=7-7");
        assert_eq!(sanitize(Some("  BYTES=0-99  ")), "bytes=0-99");
    }

    #[test]
    fn multi_range_is_dropped() {
        assert_eq!(sanitize(Some("bytes=0-1,2-3")), "");
        assert_eq!(sanitize(Some("bytes=0-1, 100-200, 300-")), "");
    }

    #[test]
    fn malformed_values_are_dropped() {
        for bad in [
            "bytes=abc-def",
            "bytes=",
            "bytes=-",
            "bytes=1-2-3",
            "0-100",
            "items=0-100",
            "bytes=1.5-2",
            "bytes=-0",
            "bytes=0x10-0x20",
            "bytes=+5-10",
            "bytes=-5-10",
            "bytes=0-10\r\nX-Injected: 1",
            "bytes=0-10\nHost: evil.example",
        ] {
            assert_eq!(sanitize(Some(bad)), "", "expected drop: {bad:?}");
        }
    }

    #[test]
    fn empty_inputs_yield_no_range() {
        assert_eq!(sanitize(None), "");
        assert_eq!(sanitize(Some("")), "");
        assert_eq!(sanitize(Some("   ")), "");
    }

    #[test]
    fn every_form_is_bounded_to_the_cap() {
        assert_eq!(
            sanitize(Some("bytes=0-999999999999")),
            format!("bytes=0-{}", CAP - 1)
        );
        assert_eq!(
            sanitize(Some("bytes=1000-999999999999")),
            format!("bytes=1000-{}", 1000 + CAP - 1)
        );
        assert_eq!(
            sanitize(Some("bytes=500-")),
            format!("bytes=500-{}", 500 + CAP - 1)
        );
        assert_eq!(sanitize(Some("bytes=-500")), "bytes=-500");
        assert_eq!(
            sanitize(Some("bytes=-999999999999")),
            format!("bytes=-{CAP}")
        );
        assert_eq!(
            sanitize(Some(&format!("bytes=0-{}", CAP - 1))),
            format!("bytes=0-{}", CAP - 1)
        );
        assert_eq!(sanitize(Some("bytes=0-1048575")), "bytes=0-1048575");
    }

    #[test]
    fn inverted_and_unsafe_positions_are_dropped() {
        assert_eq!(sanitize(Some("bytes=500-100")), "");
        assert_eq!(sanitize(Some("bytes=99999999999999999-")), "");
        assert_eq!(sanitize(Some(&format!("bytes=0-{}", "9".repeat(64)))), "");
    }
}
