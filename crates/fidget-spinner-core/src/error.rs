use thiserror::Error;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CoreError {
    #[error("text values must not be blank")]
    EmptyText,
    #[error("metric units must not be blank")]
    EmptyMetricUnit,
    #[error(
        "invalid metric unit `{0}`; expected a built-in unit like `microseconds` or a lowercase ascii token"
    )]
    InvalidMetricUnit(String),
    #[error("tag names must not be blank")]
    EmptyTagName,
    #[error(
        "invalid tag name `{0}`; expected lowercase ascii alphanumerics separated by `-`, `_`, or `/`"
    )]
    InvalidTagName(String),
    #[error("slug values must not be blank")]
    EmptySlug,
    #[error("invalid slug `{0}`; expected lowercase ascii alphanumerics separated by `-` or `_`")]
    InvalidSlug(String),
    #[error("slug `{0}` is ambiguous with a UUID selector")]
    UuidLikeSlug(String),
    #[error("command recipes must contain at least one argv element")]
    EmptyCommand,
}
