use fidget_spinner_core::{KnownMetricUnit, MetricDisplayUnit, MetricQuantity};

use crate::ui::number::format_significant;

const TARGET_TICKS: f64 = 6.0;

#[derive(Clone, Debug, PartialEq)]
pub(super) struct AxisUnit {
    label: String,
    canonical_per_unit: f64,
    integer_observations: bool,
}

impl AxisUnit {
    pub(super) fn choose(
        quantity: &MetricQuantity,
        display_units: impl Iterator<Item = MetricDisplayUnit>,
        magnitude: f64,
    ) -> Self {
        if quantity == &MetricQuantity::time() {
            return Self::from_ladder(
                magnitude,
                &[
                    ("nanoseconds", 1.0),
                    ("microseconds", 1_000.0),
                    ("milliseconds", 1_000_000.0),
                    ("seconds", 1_000_000_000.0),
                    ("minutes", 60_000_000_000.0),
                    ("hours", 3_600_000_000_000.0),
                ],
            );
        }
        if quantity == &MetricQuantity::byte() {
            return Self::from_ladder(
                magnitude,
                &[
                    ("bytes", 1.0),
                    ("kibibytes", 1_024.0),
                    ("mebibytes", 1_048_576.0),
                    ("gibibytes", 1_073_741_824.0),
                ],
            );
        }
        if quantity == &MetricQuantity::count() {
            return Self::integer("count");
        }
        if quantity.is_dimensionless()
            && display_units.into_iter().any(|unit| {
                matches!(
                    unit,
                    MetricDisplayUnit::Known(unit)
                        if unit.known_kind() == Some(KnownMetricUnit::Percent)
                )
            })
        {
            return Self::new("percent", 0.01);
        }
        Self::new(quantity.canonical_unit_label(), 1.0)
    }

    fn from_ladder(magnitude: f64, ladder: &[(&str, f64)]) -> Self {
        let magnitude = magnitude.abs();
        let (label, factor) = ladder
            .iter()
            .rev()
            .find(|(_, factor)| magnitude >= *factor)
            .copied()
            .unwrap_or(ladder[0]);
        Self::new(label, factor)
    }

    fn new(label: impl Into<String>, canonical_per_unit: f64) -> Self {
        Self {
            label: label.into(),
            canonical_per_unit,
            integer_observations: false,
        }
    }

    fn integer(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            canonical_per_unit: 1.0,
            integer_observations: true,
        }
    }

    pub(super) fn display_value(&self, canonical: f64) -> f64 {
        canonical / self.canonical_per_unit
    }

    pub(super) fn label(&self) -> &str {
        &self.label
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AxisScale {
    Linear,
    Logarithmic,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct AxisTick {
    pub(super) value: f64,
    pub(super) label: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct ValueAxisPlan {
    pub(super) unit: AxisUnit,
    pub(super) scale: AxisScale,
    pub(super) minimum: f64,
    pub(super) maximum: f64,
    pub(super) ticks: Vec<AxisTick>,
}

impl ValueAxisPlan {
    pub(super) fn build(
        quantity: MetricQuantity,
        display_units: impl Iterator<Item = MetricDisplayUnit>,
        canonical_values: &[f64],
        request_logarithmic: bool,
    ) -> Option<Self> {
        let magnitude = canonical_values
            .iter()
            .copied()
            .filter(|value| value.is_finite())
            .map(f64::abs)
            .fold(0.0, f64::max);
        let unit = AxisUnit::choose(&quantity, display_units, magnitude);
        let values = canonical_values
            .iter()
            .copied()
            .filter(|value| value.is_finite())
            .map(|value| unit.display_value(value))
            .collect::<Vec<_>>();
        if values.is_empty() {
            return None;
        }
        let logarithmic = request_logarithmic && values.iter().all(|value| *value > 0.0);
        let (minimum, maximum, ticks) = if logarithmic {
            logarithmic_domain_and_ticks(&values)?
        } else {
            linear_domain_and_ticks(&values)?
        };
        Some(Self {
            unit,
            scale: if logarithmic {
                AxisScale::Logarithmic
            } else {
                AxisScale::Linear
            },
            minimum,
            maximum,
            ticks,
        })
    }

    pub(super) fn supports_logarithmic(values: &[f64]) -> bool {
        !values.is_empty() && values.iter().all(|value| value.is_finite() && *value > 0.0)
    }

    pub(super) fn position(&self, canonical: f64, top: f64, bottom: f64) -> Option<f64> {
        let value = self.unit.display_value(canonical);
        self.position_display(value, top, bottom)
    }

    pub(super) fn position_display(&self, value: f64, top: f64, bottom: f64) -> Option<f64> {
        if !value.is_finite() {
            return None;
        }
        let ratio = match self.scale {
            AxisScale::Linear => (value - self.minimum) / (self.maximum - self.minimum),
            AxisScale::Logarithmic if value > 0.0 => {
                (value.log10() - self.minimum.log10())
                    / (self.maximum.log10() - self.minimum.log10())
            }
            AxisScale::Logarithmic => return None,
        };
        Some(bottom - ratio * (bottom - top))
    }

    pub(super) fn format_value(&self, canonical: f64) -> String {
        let display = self.unit.display_value(canonical);
        let step = self
            .ticks
            .windows(2)
            .next()
            .map_or(1.0, |ticks| (ticks[1].value - ticks[0].value).abs());
        let number = if self.unit.integer_observations && display.fract() == 0.0 {
            format_axis_number(display, 1.0)
        } else {
            format_observation_number(display, step)
        };
        format!("{} {}", number, self.unit.label())
    }
}

fn linear_domain_and_ticks(values: &[f64]) -> Option<(f64, f64, Vec<AxisTick>)> {
    let minimum = values.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    if !minimum.is_finite() || !maximum.is_finite() {
        return None;
    }
    let span = maximum - minimum;
    let raw_step = if span > 0.0 {
        span / (TARGET_TICKS - 1.0)
    } else {
        minimum.abs().max(1.0) / (TARGET_TICKS - 1.0)
    };
    let step = nice_step(raw_step);
    let mut domain_minimum = (minimum / step).floor() * step;
    let mut domain_maximum = (maximum / step).ceil() * step;
    if domain_minimum == domain_maximum {
        domain_minimum -= step;
        domain_maximum += step;
    }
    let mut ticks = Vec::new();
    let mut value = domain_minimum;
    for _ in 0..128 {
        if value > domain_maximum + step * 1e-9 {
            break;
        }
        ticks.push(AxisTick {
            value,
            label: format_axis_number(value, step),
        });
        value += step;
    }
    Some((domain_minimum, domain_maximum, ticks))
}

fn logarithmic_domain_and_ticks(values: &[f64]) -> Option<(f64, f64, Vec<AxisTick>)> {
    let minimum = values.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    if !minimum.is_finite() || !maximum.is_finite() || minimum <= 0.0 {
        return None;
    }
    let mut domain_minimum = 10.0_f64.powf(minimum.log10().floor());
    let mut domain_maximum = 10.0_f64.powf(maximum.log10().ceil());
    if domain_minimum == domain_maximum {
        domain_minimum /= 10.0;
        domain_maximum *= 10.0;
    }
    let first_power = domain_minimum.log10().floor() as i32;
    let last_power = domain_maximum.log10().ceil() as i32;
    let mut ticks = Vec::new();
    for power in first_power..=last_power {
        let decade = 10.0_f64.powi(power);
        for multiplier in [1.0, 2.0, 5.0] {
            let value = decade * multiplier;
            if value >= domain_minimum && value <= domain_maximum {
                ticks.push(AxisTick {
                    value,
                    label: format_axis_number(value, decade),
                });
            }
        }
    }
    Some((domain_minimum, domain_maximum, ticks))
}

fn nice_step(raw: f64) -> f64 {
    if !raw.is_finite() || raw <= 0.0 {
        return 1.0;
    }
    let power = 10.0_f64.powf(raw.log10().floor());
    let fraction = raw / power;
    let nice_fraction = if fraction <= 1.0 {
        1.0
    } else if fraction <= 2.0 {
        2.0
    } else if fraction <= 2.5 {
        2.5
    } else if fraction <= 5.0 {
        5.0
    } else {
        10.0
    };
    nice_fraction * power
}

pub(super) fn format_axis_number(value: f64, step: f64) -> String {
    let absolute = value.abs();
    for (threshold, suffix) in [(1e9, "G"), (1e6, "M"), (1e3, "k")] {
        if absolute >= threshold {
            return trim_decimal(value / threshold, 2) + suffix;
        }
    }
    trim_decimal(value, axis_decimals(step))
}

fn format_observation_number(value: f64, step: f64) -> String {
    let absolute = value.abs();
    for (threshold, suffix) in [(1e9, "G"), (1e6, "M"), (1e3, "k")] {
        if absolute >= threshold {
            return format!(
                "{}{}",
                format_significant(value / threshold, axis_decimals(step / threshold)),
                suffix
            );
        }
    }
    format_significant(value, axis_decimals(step))
}

fn axis_decimals(step: f64) -> usize {
    if step >= 1.0 {
        0
    } else {
        usize::try_from((-step.log10().floor() as i32 + 1).clamp(0, 8)).unwrap_or(8)
    }
}

fn trim_decimal(value: f64, decimals: usize) -> String {
    let mut rendered = format!("{value:.decimals$}");
    if rendered.contains('.') {
        while rendered.ends_with('0') {
            let _ = rendered.pop();
        }
        if rendered.ends_with('.') {
            let _ = rendered.pop();
        }
    }
    if rendered == "-0" {
        "0".to_owned()
    } else {
        rendered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_axis_uses_visible_magnitude() {
        let axis = ValueAxisPlan::build(
            MetricQuantity::time(),
            std::iter::empty(),
            &[30_000_000_000_000.0],
            false,
        );
        assert_eq!(axis.map(|axis| axis.unit.label), Some("hours".to_owned()));
    }

    #[test]
    fn observation_labels_preserve_precision_after_unit_promotion() {
        let canonical = 8.456 * 60_000_000_000.0;
        let axis = ValueAxisPlan::build(
            MetricQuantity::time(),
            std::iter::empty(),
            &[canonical],
            false,
        );

        assert_eq!(axis.as_ref().map(|axis| axis.unit.label()), Some("minutes"));
        assert_eq!(
            axis.map(|axis| axis.format_value(canonical)),
            Some("8.46 minutes".to_owned())
        );
    }

    #[test]
    fn integral_counts_do_not_acquire_counterfeit_fractional_precision() {
        let canonical = 20.0;
        let axis = ValueAxisPlan::build(
            MetricQuantity::count(),
            std::iter::empty(),
            &[canonical],
            false,
        );

        assert_eq!(
            axis.map(|axis| axis.format_value(canonical)),
            Some("20 count".to_owned())
        );
    }

    #[test]
    fn logarithmic_ticks_use_canonical_subdivisions() {
        let (_, _, ticks) = logarithmic_domain_and_ticks(&[0.91, 1_000.0]).unwrap_or_default();
        assert!(ticks.iter().any(|tick| tick.value == 2.0));
        assert!(ticks.iter().any(|tick| tick.value == 500.0));
    }
}
