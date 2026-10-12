//! JavaScript number-conversion semantics the JS providers relied on.

/// `Number(value)` for a string: trims, `""` is 0, accepts decimal,
/// exponent, `Infinity` and `0x`/`0o`/`0b` integers; anything else is NaN.
pub fn number(value: &str) -> f64 {
    let s = value.trim();
    if s.is_empty() {
        return 0.0;
    }
    let (sign, body) = match s.as_bytes()[0] {
        b'-' => (-1.0, &s[1..]),
        b'+' => (1.0, &s[1..]),
        _ => (1.0, s),
    };
    if body == "Infinity" {
        return sign * f64::INFINITY;
    }
    let radix = |prefix: &str, radix: u32| -> Option<f64> {
        let rest = body
            .strip_prefix(prefix)
            .or_else(|| body.strip_prefix(&prefix.to_uppercase()))?;
        // Signed radix literals are NaN in JS.
        if sign < 0.0 || s.starts_with('+') {
            return Some(f64::NAN);
        }
        Some(u128::from_str_radix(rest, radix).map_or(f64::NAN, |v| v as f64))
    };
    for (prefix, base) in [("0x", 16), ("0o", 8), ("0b", 2)] {
        if let Some(value) = radix(prefix, base) {
            return value;
        }
    }
    // Rust accepts "inf"/"nan"/"infinity"; JS does not.
    if !body
        .bytes()
        .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-'))
    {
        return f64::NAN;
    }
    if body.starts_with('.') && body.len() == 1 {
        return f64::NAN;
    }
    body.parse::<f64>().map_or(f64::NAN, |v| sign * v)
}

/// `parseInt(value, 10)`: leading whitespace, optional sign, then the
/// longest run of digits. `None` stands for NaN.
pub fn parse_int(value: &str) -> Option<i64> {
    let s = value.trim_start();
    let (negative, rest) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    let parsed = digits.parse::<i64>().unwrap_or(i64::MAX);
    Some(if negative { -parsed } else { parsed })
}

/// `parseFloat(value)`: the longest leading decimal literal.
pub fn parse_float(value: &str) -> f64 {
    let s = value.trim_start();
    if let Some(rest) = s.strip_prefix("Infinity").or(s.strip_prefix("+Infinity")) {
        let _ = rest;
        return f64::INFINITY;
    }
    if s.starts_with("-Infinity") {
        return f64::NEG_INFINITY;
    }
    let bytes = s.as_bytes();
    let mut end = 0;
    let mut seen_digit = false;
    let mut seen_dot = false;
    let mut i = 0;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        i += 1;
    }
    while i < bytes.len() {
        match bytes[i] {
            b'0'..=b'9' => {
                seen_digit = true;
                end = i + 1;
            }
            b'.' if !seen_dot => seen_dot = true,
            b'e' | b'E' if seen_digit => {
                let mut j = i + 1;
                if j < bytes.len() && (bytes[j] == b'+' || bytes[j] == b'-') {
                    j += 1;
                }
                let start = j;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j > start {
                    end = j;
                }
                break;
            }
            _ => break,
        }
        i += 1;
    }
    if !seen_digit {
        return f64::NAN;
    }
    s[..end].parse::<f64>().unwrap_or(f64::NAN)
}

/// Milliseconds since the Unix epoch, like `Date.now()`.
pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_matches_js() {
        assert_eq!(number(""), 0.0);
        assert_eq!(number(" 12 "), 12.0);
        assert_eq!(number("1e3"), 1000.0);
        assert_eq!(number("0x10"), 16.0);
        assert_eq!(number("-2.5"), -2.5);
        assert!(number("12px").is_nan());
        assert!(number("inf").is_nan());
        assert_eq!(number("-Infinity"), f64::NEG_INFINITY);
    }

    #[test]
    fn parse_int_matches_js() {
        assert_eq!(parse_int("42abc"), Some(42));
        assert_eq!(parse_int("  -7"), Some(-7));
        assert_eq!(parse_int("abc"), None);
        assert_eq!(parse_int("3.9"), Some(3));
    }

    #[test]
    fn parse_float_matches_js() {
        assert_eq!(parse_float("3.5km"), 3.5);
        assert_eq!(parse_float("-1e2x"), -100.0);
        assert!(parse_float("x").is_nan());
    }
}
