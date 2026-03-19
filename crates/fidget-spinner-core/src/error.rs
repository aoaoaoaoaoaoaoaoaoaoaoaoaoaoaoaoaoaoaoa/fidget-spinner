use thiserror::Error;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CoreError {
    #[error("text values must not be blank")]
    EmptyText,
    #[error("tag names must not be blank")]
    EmptyTagName,
    #[error(
        "invalid tag name `{0}`; expected lowercase ascii alphanumerics separated by `-`, `_`, or `/`"
    )]
    InvalidTagName(String),
    #[error("command recipes must contain at least one argv element")]
    EmptyCommand,
}
