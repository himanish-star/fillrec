//! Exact fixed-point parsing for prices and quantities.
//!
//! Venues send decimals as strings ("67123.45000000"). Going through `f64` loses
//! exactness (0.1 + 0.2 != 0.3) and is slower than a direct digit loop, so every
//! price and quantity is stored as an `i64` scaled by 10^8. This maps 1:1 onto a
//! ClickHouse `Decimal64(8)` column.

pub const DECIMALS: u32 = 8;
pub const SCALE: i64 = 100_000_000;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FixedError {
    Empty,
    Invalid,
    Overflow,
    /// Non-zero digit beyond 8 decimal places. We refuse rather than silently truncate:
    /// a ledger must never round without saying so.
    Precision,
}

impl std::fmt::Display for FixedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for FixedError {}

#[inline]
pub fn parse_fixed(s: &str) -> Result<i64, FixedError> {
    let mut b = s.as_bytes();
    if b.is_empty() {
        return Err(FixedError::Empty);
    }
    let neg = b[0] == b'-';
    if neg {
        b = &b[1..];
    }
    let mut int: i64 = 0;
    let mut i = 0;
    let mut digits = 0;
    while i < b.len() && b[i] != b'.' {
        let d = b[i].wrapping_sub(b'0');
        if d > 9 {
            return Err(FixedError::Invalid);
        }
        int = int
            .checked_mul(10)
            .and_then(|v| v.checked_add(d as i64))
            .ok_or(FixedError::Overflow)?;
        i += 1;
        digits += 1;
    }
    let mut frac: i64 = 0;
    let mut fd: u32 = 0;
    if i < b.len() {
        i += 1; // '.'
        while i < b.len() {
            let d = b[i].wrapping_sub(b'0');
            if d > 9 {
                return Err(FixedError::Invalid);
            }
            if fd < DECIMALS {
                frac = frac * 10 + d as i64;
                fd += 1;
            } else if d != 0 {
                return Err(FixedError::Precision);
            }
            i += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return Err(FixedError::Invalid);
    }
    frac *= 10i64.pow(DECIMALS - fd);
    let v = int
        .checked_mul(SCALE)
        .and_then(|v| v.checked_add(frac))
        .ok_or(FixedError::Overflow)?;
    Ok(if neg { -v } else { v })
}

pub fn format_fixed(v: i64) -> String {
    let a = v.unsigned_abs();
    let s = SCALE as u64;
    format!("{}{}.{:08}", if v < 0 { "-" } else { "" }, a / s, a % s)
}

/// Parse an unsigned integer that a venue sends as a string ("1750428146322").
#[inline]
pub fn parse_u64(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    if b.is_empty() || b.len() > 19 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
    }
    Some(v)
}

/// Minimal RFC 3339 UTC parser ("2026-11-21T10:31:02.123456Z") -> microseconds since epoch.
/// Coinbase sends exactly this shape on every match; a general date library is ~10x slower here.
pub fn parse_rfc3339_us(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 20 {
        return None;
    }
    let num = |from: usize, to: usize| -> Option<i64> {
        let mut v = 0i64;
        for &c in &b[from..to] {
            let d = c.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v * 10 + d as i64;
        }
        Some(v)
    };
    if b[4] != b'-' || b[7] != b'-' || (b[10] != b'T' && b[10] != b' ') || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let (y, m, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (h, mi, se) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    let mut i = 19;
    let mut us = 0i64;
    if b[i] == b'.' {
        i += 1;
        let mut n = 0;
        while i < b.len() && b[i].is_ascii_digit() {
            if n < 6 {
                us = us * 10 + (b[i] - b'0') as i64;
                n += 1;
            }
            i += 1;
        }
        while n < 6 {
            us *= 10;
            n += 1;
        }
    }
    match &b[i..] {
        b"Z" | b"+00:00" => {}
        _ => return None,
    }
    let days = days_from_civil(y, m, d);
    Some((days * 86_400 + h * 3_600 + mi * 60 + se) * 1_000_000 + us)
}

/// Howard Hinnant's days_from_civil.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exact() {
        assert_eq!(parse_fixed("67123.45000000"), Ok(6_712_345_000_000));
        assert_eq!(parse_fixed("0.00001"), Ok(1_000));
        assert_eq!(parse_fixed("0.456"), Ok(45_600_000));
        assert_eq!(parse_fixed("12"), Ok(1_200_000_000));
        assert_eq!(parse_fixed("-1.5"), Ok(-150_000_000));
        assert_eq!(parse_fixed(".5"), Ok(50_000_000));
        assert_eq!(parse_fixed("1.123456780000"), Ok(112_345_678));
    }

    #[test]
    fn rejects_bad_input() {
        assert_eq!(parse_fixed(""), Err(FixedError::Empty));
        assert_eq!(parse_fixed("1e-5"), Err(FixedError::Invalid));
        assert_eq!(parse_fixed("."), Err(FixedError::Invalid));
        assert_eq!(parse_fixed("1.000000001"), Err(FixedError::Precision));
        assert_eq!(parse_fixed("99999999999999999"), Err(FixedError::Overflow));
    }

    #[test]
    fn roundtrips() {
        assert_eq!(format_fixed(6_712_345_000_000), "67123.45000000");
        assert_eq!(format_fixed(-150_000_000), "-1.50000000");
    }

    #[test]
    fn rfc3339() {
        // 2014-11-07T08:19:27.028459Z == 1415348367.028459
        assert_eq!(parse_rfc3339_us("2014-11-07T08:19:27.028459Z"), Some(1_415_348_367_028_459));
        assert_eq!(parse_rfc3339_us("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_rfc3339_us("2024-02-29T23:59:59.5Z"), Some(1_709_251_199_500_000));
        assert_eq!(parse_rfc3339_us("2024-02-29T23:59:59.5+05:30"), None);
    }
}
