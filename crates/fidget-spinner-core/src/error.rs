use thiserror::Error;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CoreError {
    #[error("text values must not be blank")]
    EmptyText,
    #[error("command recipes must contain at least one argv element")]
    EmptyCommand,
}
