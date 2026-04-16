use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! define_id {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            #[must_use]
            pub fn fresh() -> Self {
                Self(Uuid::now_v7())
            }

            #[must_use]
            pub fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            #[must_use]
            pub fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                Display::fmt(&self.0, formatter)
            }
        }
    };
}

define_id!(ArtifactId);
define_id!(ExperimentId);
define_id!(FrontierId);
define_id!(HypothesisId);
define_id!(KpiId);
define_id!(MetricId);
define_id!(RegistryLockId);
define_id!(TagFamilyId);
define_id!(TagId);
