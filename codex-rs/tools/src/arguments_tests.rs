use crate::arguments::i32;
use crate::arguments::i64;
use crate::arguments::option_i32;
use crate::arguments::option_i64;
use crate::arguments::option_u64;
use crate::arguments::option_usize;
use crate::arguments::u64;
use crate::arguments::usize;
use pretty_assertions::assert_eq;
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
struct AllTargets {
    #[serde(deserialize_with = "u64::deserialize")]
    unsigned_u64: std::primitive::u64,
    #[serde(deserialize_with = "i64::deserialize")]
    signed_i64: std::primitive::i64,
    #[serde(deserialize_with = "i32::deserialize")]
    signed_i32: std::primitive::i32,
    #[serde(deserialize_with = "usize::deserialize")]
    unsigned_usize: std::primitive::usize,
}

#[derive(Debug, Deserialize, PartialEq)]
struct SignedTargets {
    #[serde(deserialize_with = "i64::deserialize")]
    signed_i64: std::primitive::i64,
    #[serde(deserialize_with = "i32::deserialize")]
    signed_i32: std::primitive::i32,
}

#[derive(Debug, Deserialize, PartialEq)]
struct U64Field {
    #[serde(deserialize_with = "u64::deserialize")]
    value: std::primitive::u64,
}

#[derive(Debug, Deserialize, PartialEq)]
struct I64Field {
    #[serde(deserialize_with = "i64::deserialize")]
    value: std::primitive::i64,
}

#[derive(Debug, Deserialize, PartialEq)]
struct I32Field {
    #[serde(deserialize_with = "i32::deserialize")]
    value: std::primitive::i32,
}

#[derive(Debug, Deserialize, PartialEq)]
struct UsizeField {
    #[serde(deserialize_with = "usize::deserialize")]
    value: std::primitive::usize,
}

#[derive(Debug, Deserialize, PartialEq)]
struct OptionalTargets {
    #[serde(default, deserialize_with = "option_u64::deserialize")]
    unsigned_u64: Option<std::primitive::u64>,
    #[serde(default, deserialize_with = "option_i64::deserialize")]
    signed_i64: Option<std::primitive::i64>,
    #[serde(default, deserialize_with = "option_i32::deserialize")]
    signed_i32: Option<std::primitive::i32>,
    #[serde(default, deserialize_with = "option_usize::deserialize")]
    unsigned_usize: Option<std::primitive::usize>,
}

fn default_budget() -> Option<std::primitive::i64> {
    Some(7)
}

#[derive(Debug, Deserialize, PartialEq)]
struct OptionalDefault {
    #[serde(
        default = "default_budget",
        deserialize_with = "option_i64::deserialize"
    )]
    budget: Option<std::primitive::i64>,
}

fn all_target_json(lexeme: &str) -> String {
    format!(
        "{{\"unsigned_u64\":{lexeme},\"signed_i64\":{lexeme},\"signed_i32\":{lexeme},\"unsigned_usize\":{lexeme}}}"
    )
}

fn field_json(lexeme: &str) -> String {
    format!("{{\"value\":{lexeme}}}")
}

#[test]
fn integer_valued_decimals_and_exponents_decode_exactly_for_all_targets() {
    for lexeme in ["60000.0", "6e4", "6.0e4", "60000"] {
        let decoded: AllTargets = serde_json::from_str(&all_target_json(lexeme)).expect(lexeme);
        assert_eq!(
            decoded,
            AllTargets {
                unsigned_u64: 60_000,
                signed_i64: 60_000,
                signed_i32: 60_000,
                unsigned_usize: 60_000,
            }
        );
    }
}

#[test]
fn negative_exponent_decimals_are_parsed_exactly() {
    for (lexeme, expected) in [("600000e-1", 60_000), ("10.0e-1", 1), ("60000.0e0", 60_000)] {
        let decoded: I64Field =
            serde_json::from_str(&field_json(lexeme)).expect("integer-valued exponent");
        assert_eq!(decoded.value, expected, "{lexeme}");
    }

    for lexeme in ["15e-1", "1e-1", "1.5e0"] {
        let error = serde_json::from_str::<I64Field>(&field_json(lexeme))
            .expect_err("fractional exponent must be rejected");
        assert!(error.to_string().contains("a fractional number"), "{error}");
    }
}

#[test]
fn negative_zero_and_negative_integers_decode_for_signed_targets() {
    let negative_zero: AllTargets = serde_json::from_str(&all_target_json("-0.0"))
        .expect("negative zero is integer-valued zero");
    assert_eq!(
        negative_zero,
        AllTargets {
            unsigned_u64: 0,
            signed_i64: 0,
            signed_i32: 0,
            unsigned_usize: 0,
        }
    );

    let negative_values: SignedTargets =
        serde_json::from_str(r#"{"signed_i64":-60000.0,"signed_i32":-6e4}"#)
            .expect("negative integer-valued decimal and exponent");
    assert_eq!(
        negative_values,
        SignedTargets {
            signed_i64: -60_000,
            signed_i32: -60_000,
        }
    );
}

#[test]
fn exact_large_integers_and_destination_boundaries_are_preserved() {
    let above_two_to_53: U64Field = serde_json::from_str(&field_json("9007199254740993"))
        .expect("integer above 2^53 remains exact");
    assert_eq!(above_two_to_53.value, 9_007_199_254_740_993);

    let maximum: U64Field =
        serde_json::from_str(&field_json(&std::primitive::u64::MAX.to_string()))
            .expect("u64 maximum");
    assert_eq!(maximum.value, std::primitive::u64::MAX);

    let minimum_signed: SignedTargets =
        serde_json::from_str(r#"{"signed_i64":-9223372036854775808,"signed_i32":-2147483648}"#)
            .expect("signed minimum values");
    assert_eq!(
        minimum_signed,
        SignedTargets {
            signed_i64: std::primitive::i64::MIN,
            signed_i32: std::primitive::i32::MIN,
        }
    );

    let maximum_signed: SignedTargets =
        serde_json::from_str(r#"{"signed_i64":9223372036854775807,"signed_i32":2147483647}"#)
            .expect("signed maximum values");
    assert_eq!(
        maximum_signed,
        SignedTargets {
            signed_i64: std::primitive::i64::MAX,
            signed_i32: std::primitive::i32::MAX,
        }
    );

    let usize_max = std::primitive::usize::MAX;
    let parsed_usize_max: UsizeField =
        serde_json::from_str(&field_json(&usize_max.to_string())).expect("usize maximum");
    assert_eq!(parsed_usize_max.value, usize_max);
}

#[test]
fn integer_overflow_is_refused_for_each_destination() {
    for error in [
        serde_json::from_str::<U64Field>(&field_json("18446744073709551616")).unwrap_err(),
        serde_json::from_str::<U64Field>(&field_json("1e20")).unwrap_err(),
        serde_json::from_str::<I32Field>(&field_json("2147483648")).unwrap_err(),
        serde_json::from_str::<I64Field>(&field_json("9223372036854775808")).unwrap_err(),
        serde_json::from_str::<I64Field>(&field_json("-9223372036854775809")).unwrap_err(),
    ] {
        assert!(error.to_string().contains("invalid value"), "{error}");
    }

    let usize_max = std::primitive::usize::MAX;
    assert!(serde_json::from_str::<UsizeField>(&field_json(&format!("{usize_max}0"))).is_err());
}

#[test]
fn fractional_and_rounded_numeric_lexemes_are_refused_for_every_target() {
    for lexeme in ["1.5", "1.0000000000000001", "9007199254740991.5"] {
        for error in [
            serde_json::from_str::<U64Field>(&field_json(lexeme)).unwrap_err(),
            serde_json::from_str::<I64Field>(&field_json(lexeme)).unwrap_err(),
            serde_json::from_str::<I32Field>(&field_json(lexeme)).unwrap_err(),
            serde_json::from_str::<UsizeField>(&field_json(lexeme)).unwrap_err(),
        ] {
            assert!(error.to_string().contains("invalid value"), "{error}");
        }
    }
}

#[test]
fn unsigned_targets_refuse_negative_nonzero_values() {
    for lexeme in ["-1", "-1.0"] {
        assert!(serde_json::from_str::<U64Field>(&field_json(lexeme)).is_err());
        assert!(serde_json::from_str::<UsizeField>(&field_json(lexeme)).is_err());
    }
}

#[test]
fn optional_integer_adapters_preserve_null_omission_and_serde_defaults() {
    let omitted: OptionalTargets =
        serde_json::from_str("{}").expect("omitted Options default to None");
    assert_eq!(
        omitted,
        OptionalTargets {
            unsigned_u64: None,
            signed_i64: None,
            signed_i32: None,
            unsigned_usize: None,
        }
    );

    let nulls: OptionalTargets = serde_json::from_str(
        r#"{"unsigned_u64":null,"signed_i64":null,"signed_i32":null,"unsigned_usize":null}"#,
    )
    .expect("null Options remain None");
    assert_eq!(nulls, omitted);

    let values: OptionalTargets = serde_json::from_str(
        r#"{"unsigned_u64":60000.0,"signed_i64":6e4,"signed_i32":6.0e4,"unsigned_usize":60000}"#,
    )
    .expect("optional integer values");
    assert_eq!(
        values,
        OptionalTargets {
            unsigned_u64: Some(60_000),
            signed_i64: Some(60_000),
            signed_i32: Some(60_000),
            unsigned_usize: Some(60_000),
        }
    );

    let defaulted: OptionalDefault = serde_json::from_str("{}").expect("serde field default");
    assert_eq!(defaulted.budget, Some(7));
}

#[test]
fn optional_integer_adapters_reject_strings_and_booleans() {
    assert!(serde_json::from_str::<OptionalTargets>(r#"{"unsigned_u64":"60000"}"#).is_err());
    assert!(serde_json::from_str::<OptionalTargets>(r#"{"signed_i64":true}"#).is_err());
}

#[test]
fn invalid_json_types_keep_serde_invalid_type_errors() {
    for lexeme in [r#""60000""#, "true", "null", "[]", "{}"] {
        let error = serde_json::from_str::<U64Field>(&field_json(lexeme)).unwrap_err();
        assert!(error.to_string().contains("invalid type"), "{error}");
        assert!(error.to_string().contains("expected u64"), "{error}");
    }
}

#[test]
fn pathological_exponents_and_long_digit_inputs_are_refused() {
    assert!(serde_json::from_str::<U64Field>(&field_json("1e999999999")).is_err());
    assert!(serde_json::from_str::<U64Field>(&field_json("1e-999999999")).is_err());

    let long_exponent = format!("1e{}", "9".repeat(10_000));
    assert!(serde_json::from_str::<U64Field>(&field_json(&long_exponent)).is_err());

    let long_integer = "9".repeat(10_000);
    assert!(serde_json::from_str::<U64Field>(&field_json(&long_integer)).is_err());
}

#[test]
fn duplicate_annotated_fields_keep_serde_rejection() {
    let error = serde_json::from_str::<U64Field>(r#"{"value":1,"value":2}"#).unwrap_err();
    assert!(
        error.to_string().contains("duplicate field `value`"),
        "{error}"
    );
}
