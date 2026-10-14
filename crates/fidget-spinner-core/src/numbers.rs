use serde::{Deserialize, Serialize};
use std::{
    fmt,
    num::{NonZeroU64, NonZeroUsize, ParseIntError},
    str::FromStr,
};

macro_rules! number {
    ($name:ident, $primitive:ty, $nonzero:ty) => {
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name($nonzero);

        impl $name {
            pub const FIRST: Self = Self(<$nonzero>::MIN);
            #[must_use]
            pub const fn new(value: $primitive) -> Option<Self> {
                match <$nonzero>::new(value) {
                    Some(value) => Some(Self(value)),
                    None => None,
                }
            }
            #[must_use]
            pub const fn get(self) -> $primitive {
                self.0.get()
            }
            #[must_use]
            pub fn next(self) -> Option<Self> {
                self.get().checked_add(1).and_then(Self::new)
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
        impl FromStr for $name {
            type Err = ParseIntError;
            fn from_str(raw: &str) -> Result<Self, Self::Err> {
                raw.parse().map(Self)
            }
        }
    };
}

number!(RegimeNumber, usize, NonZeroUsize);
number!(EpochNumber, u64, NonZeroU64);

impl RegimeNumber {
    pub(crate) const fn index(self) -> usize {
        self.get() - 1
    }
}
