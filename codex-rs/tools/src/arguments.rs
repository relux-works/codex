//! Serde adapters for exact integer-valued JSON number lexemes.
//!
//! These adapters accept JSON numeric lexemes whose mathematical value is an
//! integer in the destination type's range. They parse the original lexeme
//! captured by `serde_json::value::RawValue`, so integers never pass through a
//! floating-point or generic JSON-value representation.

use serde::de::Deserialize;
use serde::de::Deserializer;
use serde::de::Error;
use serde::de::Expected;
use serde::de::Unexpected;
use serde::de::{self};
use serde_json::value::RawValue;
use std::fmt;

/// Deserializes an exact integer-valued JSON number into `u64`.
pub mod u64 {
    use serde::Deserializer;

    /// Deserializes an exact integer-valued JSON number into `u64`.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<std::primitive::u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        super::deserialize_required::<D, std::primitive::u64>(deserializer)
    }
}

/// Deserializes an exact integer-valued JSON number into `i64`.
pub mod i64 {
    use serde::Deserializer;

    /// Deserializes an exact integer-valued JSON number into `i64`.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<std::primitive::i64, D::Error>
    where
        D: Deserializer<'de>,
    {
        super::deserialize_required::<D, std::primitive::i64>(deserializer)
    }
}

/// Deserializes an exact integer-valued JSON number into `i32`.
pub mod i32 {
    use serde::Deserializer;

    /// Deserializes an exact integer-valued JSON number into `i32`.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<std::primitive::i32, D::Error>
    where
        D: Deserializer<'de>,
    {
        super::deserialize_required::<D, std::primitive::i32>(deserializer)
    }
}

/// Deserializes an exact integer-valued JSON number into `usize`.
pub mod usize {
    use serde::Deserializer;

    /// Deserializes an exact integer-valued JSON number into `usize`.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<std::primitive::usize, D::Error>
    where
        D: Deserializer<'de>,
    {
        super::deserialize_required::<D, std::primitive::usize>(deserializer)
    }
}

/// Deserializes an optional exact integer-valued JSON number into `Option<u64>`.
pub mod option_u64 {
    use serde::Deserializer;

    /// Deserializes an optional exact integer-valued JSON number into `Option<u64>`.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<std::primitive::u64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        super::deserialize_option::<D, std::primitive::u64>(deserializer)
    }
}

/// Deserializes an optional exact integer-valued JSON number into `Option<i64>`.
pub mod option_i64 {
    use serde::Deserializer;

    /// Deserializes an optional exact integer-valued JSON number into `Option<i64>`.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<std::primitive::i64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        super::deserialize_option::<D, std::primitive::i64>(deserializer)
    }
}

/// Deserializes an optional exact integer-valued JSON number into `Option<i32>`.
pub mod option_i32 {
    use serde::Deserializer;

    /// Deserializes an optional exact integer-valued JSON number into `Option<i32>`.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<std::primitive::i32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        super::deserialize_option::<D, std::primitive::i32>(deserializer)
    }
}

/// Deserializes an optional exact integer-valued JSON number into `Option<usize>`.
pub mod option_usize {
    use serde::Deserializer;

    /// Deserializes an optional exact integer-valued JSON number into `Option<usize>`.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<std::primitive::usize>, D::Error>
    where
        D: Deserializer<'de>,
    {
        super::deserialize_option::<D, std::primitive::usize>(deserializer)
    }
}

/// Converts parsed sign-and-magnitude integers into one supported target type.
///
/// Implementations must accept negative zero as zero and reject any other value
/// that the target type cannot represent.
trait IntegerTarget: Sized {
    const NAME: &'static str;

    fn from_sign_and_magnitude(negative: bool, magnitude: std::primitive::u64) -> Option<Self>;
}

impl IntegerTarget for std::primitive::u64 {
    const NAME: &'static str = "u64";

    fn from_sign_and_magnitude(negative: bool, magnitude: std::primitive::u64) -> Option<Self> {
        if negative && magnitude != 0 {
            None
        } else {
            Some(magnitude)
        }
    }
}

impl IntegerTarget for std::primitive::i64 {
    const NAME: &'static str = "i64";

    fn from_sign_and_magnitude(negative: bool, magnitude: std::primitive::u64) -> Option<Self> {
        if negative {
            let min_magnitude = (std::primitive::i64::MAX as std::primitive::u64) + 1;
            match magnitude {
                value if value == min_magnitude => Some(std::primitive::i64::MIN),
                value if value <= std::primitive::i64::MAX as std::primitive::u64 => {
                    Some(-(value as std::primitive::i64))
                }
                _ => None,
            }
        } else {
            std::primitive::i64::try_from(magnitude).ok()
        }
    }
}

impl IntegerTarget for std::primitive::i32 {
    const NAME: &'static str = "i32";

    fn from_sign_and_magnitude(negative: bool, magnitude: std::primitive::u64) -> Option<Self> {
        if negative {
            let min_magnitude = (std::primitive::i32::MAX as std::primitive::u64) + 1;
            match magnitude {
                value if value == min_magnitude => Some(std::primitive::i32::MIN),
                value if value <= std::primitive::i32::MAX as std::primitive::u64 => {
                    Some(-(value as std::primitive::i32))
                }
                _ => None,
            }
        } else {
            std::primitive::i32::try_from(magnitude).ok()
        }
    }
}

impl IntegerTarget for std::primitive::usize {
    const NAME: &'static str = "usize";

    fn from_sign_and_magnitude(negative: bool, magnitude: std::primitive::u64) -> Option<Self> {
        if negative && magnitude != 0 {
            None
        } else {
            std::primitive::usize::try_from(magnitude).ok()
        }
    }
}

fn deserialize_required<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: IntegerTarget,
{
    let raw = <&'de RawValue>::deserialize(deserializer)?;
    let expected = ExpectedInteger(T::NAME);
    let Some(parsed) = parse_raw_integer(raw.get()).map_err(|error| error.into_serde(&expected))?
    else {
        return Err(D::Error::invalid_type(Unexpected::Unit, &expected));
    };

    T::from_sign_and_magnitude(parsed.negative, parsed.magnitude)
        .ok_or_else(|| D::Error::invalid_value(unexpected_integer(parsed), &expected))
}

fn deserialize_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: IntegerTarget,
{
    let raw = <&'de RawValue>::deserialize(deserializer)?;
    let expected = ExpectedInteger(T::NAME);
    let Some(parsed) = parse_raw_integer(raw.get()).map_err(|error| error.into_serde(&expected))?
    else {
        return Ok(None);
    };

    T::from_sign_and_magnitude(parsed.negative, parsed.magnitude)
        .map(Some)
        .ok_or_else(|| D::Error::invalid_value(unexpected_integer(parsed), &expected))
}

struct ExpectedInteger(&'static str);

impl Expected for ExpectedInteger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

#[derive(Clone, Copy)]
struct SignMagnitude {
    negative: bool,
    magnitude: std::primitive::u64,
}

enum ParseError {
    InvalidType(UnexpectedKind),
    InvalidValue(Unexpected<'static>),
}

impl ParseError {
    fn into_serde<E: de::Error>(self, expected: &ExpectedInteger) -> E {
        match self {
            Self::InvalidType(kind) => E::invalid_type(kind.into_unexpected(), expected),
            Self::InvalidValue(unexpected) => E::invalid_value(unexpected, expected),
        }
    }
}

enum UnexpectedKind {
    Boolean(bool),
    String,
    Sequence,
    Map,
}

impl UnexpectedKind {
    fn into_unexpected(self) -> Unexpected<'static> {
        match self {
            Self::Boolean(value) => Unexpected::Bool(value),
            Self::String => Unexpected::Other("string"),
            Self::Sequence => Unexpected::Seq,
            Self::Map => Unexpected::Map,
        }
    }
}

fn parse_raw_integer(raw: &str) -> Result<Option<SignMagnitude>, ParseError> {
    let raw = raw.trim_matches(|character| matches!(character, ' ' | '\n' | '\r' | '\t'));
    match raw.as_bytes().first().copied() {
        Some(b'n') if raw == "null" => Ok(None),
        Some(b't') if raw == "true" => Err(ParseError::InvalidType(UnexpectedKind::Boolean(true))),
        Some(b'f') if raw == "false" => {
            Err(ParseError::InvalidType(UnexpectedKind::Boolean(false)))
        }
        Some(b'"') => Err(ParseError::InvalidType(UnexpectedKind::String)),
        Some(b'[') => Err(ParseError::InvalidType(UnexpectedKind::Sequence)),
        Some(b'{') => Err(ParseError::InvalidType(UnexpectedKind::Map)),
        Some(b'-' | b'0'..=b'9') => parse_number(raw.as_bytes()).map(Some),
        _ => Err(ParseError::InvalidValue(Unexpected::Other(
            "a malformed number",
        ))),
    }
}

fn parse_number(raw: &[u8]) -> Result<SignMagnitude, ParseError> {
    let mut index = 0;
    let negative = raw.first() == Some(&b'-');
    if negative {
        index += 1;
    }

    let mut total_digits = 0usize;
    let mut fraction_digits = 0usize;
    let mut leading_zero_digits = 0usize;
    let mut trailing_zero_digits = 0usize;
    let mut has_nonzero_digit = false;

    while raw.get(index).is_some_and(u8::is_ascii_digit) {
        record_coefficient_digit(
            raw[index],
            &mut total_digits,
            &mut leading_zero_digits,
            &mut trailing_zero_digits,
            &mut has_nonzero_digit,
        );
        index += 1;
    }

    if raw.get(index) == Some(&b'.') {
        index += 1;
        while raw.get(index).is_some_and(u8::is_ascii_digit) {
            record_coefficient_digit(
                raw[index],
                &mut total_digits,
                &mut leading_zero_digits,
                &mut trailing_zero_digits,
                &mut has_nonzero_digit,
            );
            fraction_digits += 1;
            index += 1;
        }
    }
    let mantissa_end = index;

    let mut exponent_negative = false;
    let mut exponent_magnitude = 0usize;
    let mut exponent_is_huge = false;
    if raw
        .get(index)
        .copied()
        .is_some_and(|byte| matches!(byte, b'e' | b'E'))
    {
        index += 1;
        if raw.get(index) == Some(&b'-') {
            exponent_negative = true;
            index += 1;
        } else if raw.get(index) == Some(&b'+') {
            index += 1;
        }

        let exponent_limit = raw.len().saturating_add(32);
        let exponent_start = index;
        while raw.get(index).is_some_and(u8::is_ascii_digit) {
            let digit = usize::from(raw[index] - b'0');
            if !exponent_is_huge {
                match exponent_magnitude
                    .checked_mul(10)
                    .and_then(|value| value.checked_add(digit))
                {
                    Some(value) if value <= exponent_limit => exponent_magnitude = value,
                    _ => exponent_is_huge = true,
                }
            }
            index += 1;
        }
        if index == exponent_start {
            return Err(ParseError::InvalidValue(Unexpected::Other(
                "a malformed number",
            )));
        }
    }

    if index != raw.len() || total_digits == 0 {
        return Err(ParseError::InvalidValue(Unexpected::Other(
            "a malformed number",
        )));
    }
    if !has_nonzero_digit {
        return Ok(SignMagnitude {
            negative,
            magnitude: 0,
        });
    }
    if exponent_is_huge {
        return Err(if exponent_negative {
            ParseError::InvalidValue(Unexpected::Other("a fractional number"))
        } else {
            ParseError::InvalidValue(Unexpected::Other("a number outside the supported range"))
        });
    }

    let (effective_digits, appended_zeroes) = if exponent_negative {
        let removed_digits = exponent_magnitude
            .checked_add(fraction_digits)
            .ok_or_else(fractional_number)?;
        if removed_digits > trailing_zero_digits {
            return Err(fractional_number());
        }
        (total_digits - removed_digits, 0)
    } else if exponent_magnitude >= fraction_digits {
        (total_digits, exponent_magnitude - fraction_digits)
    } else {
        let removed_digits = fraction_digits - exponent_magnitude;
        if removed_digits > trailing_zero_digits {
            return Err(fractional_number());
        }
        (total_digits - removed_digits, 0)
    };

    let significant_digits =
        effective_digits
            .checked_sub(leading_zero_digits)
            .ok_or(ParseError::InvalidValue(Unexpected::Other(
                "a malformed number",
            )))?;
    if significant_digits > 20 || appended_zeroes > 20 - significant_digits {
        return Err(out_of_range());
    }

    let mut magnitude = 0u64;
    let mut parsed_digits = 0usize;
    for digit in raw[usize::from(negative)..mantissa_end]
        .iter()
        .copied()
        .filter(u8::is_ascii_digit)
        .take(effective_digits)
    {
        magnitude = magnitude
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(digit - b'0')))
            .ok_or_else(out_of_range)?;
        parsed_digits += 1;
    }
    if parsed_digits != effective_digits {
        return Err(ParseError::InvalidValue(Unexpected::Other(
            "a malformed number",
        )));
    }
    for _ in 0..appended_zeroes {
        magnitude = magnitude.checked_mul(10).ok_or_else(out_of_range)?;
    }

    Ok(SignMagnitude {
        negative,
        magnitude,
    })
}

fn record_coefficient_digit(
    digit: u8,
    total_digits: &mut usize,
    leading_zero_digits: &mut usize,
    trailing_zero_digits: &mut usize,
    has_nonzero_digit: &mut bool,
) {
    *total_digits += 1;
    if !*has_nonzero_digit {
        if digit == b'0' {
            *leading_zero_digits += 1;
        } else {
            *has_nonzero_digit = true;
        }
    }
    if digit == b'0' {
        *trailing_zero_digits += 1;
    } else {
        *trailing_zero_digits = 0;
    }
}

fn fractional_number() -> ParseError {
    ParseError::InvalidValue(Unexpected::Other("a fractional number"))
}

fn out_of_range() -> ParseError {
    ParseError::InvalidValue(Unexpected::Other("a number outside the supported range"))
}

fn unexpected_integer(value: SignMagnitude) -> Unexpected<'static> {
    if !value.negative {
        return Unexpected::Unsigned(value.magnitude);
    }

    let min_magnitude = (std::primitive::i64::MAX as std::primitive::u64) + 1;
    if value.magnitude == min_magnitude {
        Unexpected::Signed(std::primitive::i64::MIN)
    } else if value.magnitude < min_magnitude {
        Unexpected::Signed(-(value.magnitude as std::primitive::i64))
    } else {
        Unexpected::Other("a negative integer outside the supported range")
    }
}

#[cfg(test)]
#[path = "arguments_tests.rs"]
mod tests;
