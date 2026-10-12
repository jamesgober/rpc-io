//! Small writers for the JSON the crate builds itself.

use alloc::string::String;

/// Appends `n` in decimal.
pub(crate) fn push_u64(out: &mut String, mut n: u64) {
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    loop {
        at -= 1;
        // `n % 10` is below 10, so the cast cannot truncate.
        digits[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    for &d in &digits[at..] {
        out.push(char::from(d));
    }
}

/// Appends `n` in decimal, with a leading `-` when negative.
pub(crate) fn push_i64(out: &mut String, n: i64) {
    if n < 0 {
        out.push('-');
    }
    push_u64(out, n.unsigned_abs());
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_numbers_cover_extremes() {
        let mut out = String::new();
        push_u64(&mut out, 0);
        out.push(' ');
        push_u64(&mut out, u64::MAX);
        out.push(' ');
        push_i64(&mut out, i64::MIN);
        out.push(' ');
        push_i64(&mut out, -7);
        assert_eq!(out, "0 18446744073709551615 -9223372036854775808 -7");
    }
}
