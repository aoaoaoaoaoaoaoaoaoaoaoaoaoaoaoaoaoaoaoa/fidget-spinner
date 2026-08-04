const MIN_SIGNIFICANT_DIGITS: usize = 3;
const MAX_FIXED_DECIMALS: usize = 8;

pub(super) fn format_significant(value: f64, minimum_fraction_digits: usize) -> String {
    if !value.is_finite() {
        return value.to_string();
    }
    if value == 0.0 {
        return fixed_zero(minimum_fraction_digits);
    }

    let integer_digits = value.abs().log10().floor() as i32 + 1;
    let significant_decimals = (MIN_SIGNIFICANT_DIGITS as i32 - integer_digits).max(0) as usize;
    let decimals = minimum_fraction_digits.max(significant_decimals);
    if decimals <= MAX_FIXED_DECIMALS {
        format!("{value:.decimals$}")
    } else {
        let decimals = MIN_SIGNIFICANT_DIGITS - 1;
        format!("{value:.decimals$e}")
    }
}

fn fixed_zero(decimals: usize) -> String {
    if decimals == 0 {
        "0".to_owned()
    } else {
        format!("0.{:0<decimals$}", "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn significant_floor_preserves_fractional_resolution() {
        assert_eq!(format_significant(8.456, 0), "8.46");
        assert_eq!(format_significant(20.0, 0), "20.0");
        assert_eq!(format_significant(123.4, 0), "123");
        assert_eq!(format_significant(0.012_345, 0), "0.0123");
    }

    #[test]
    fn fixed_fraction_policy_survives_significant_floor() {
        assert_eq!(format_significant(12.0, 3), "12.000");
        assert_eq!(format_significant(0.0, 3), "0.000");
        assert_eq!(format_significant(-0.0, 3), "0.000");
    }

    #[test]
    fn tiny_values_contract_to_scientific_not_zero() {
        assert_eq!(format_significant(1.234e-12, 0), "1.23e-12");
    }
}
