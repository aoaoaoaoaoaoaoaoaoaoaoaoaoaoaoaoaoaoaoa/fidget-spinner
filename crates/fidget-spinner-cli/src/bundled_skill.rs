use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BundledSkill {
    pub name: &'static str,
    pub description: &'static str,
    pub resource_uri: &'static str,
    pub body: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct BundledSkillSummary {
    pub name: &'static str,
    pub description: &'static str,
    pub resource_uri: &'static str,
}

impl BundledSkill {
    #[must_use]
    pub const fn summary(self) -> BundledSkillSummary {
        BundledSkillSummary {
            name: self.name,
            description: self.description,
            resource_uri: self.resource_uri,
        }
    }
}

const BUNDLED_SKILLS: [BundledSkill; 2] = [
    BundledSkill {
        name: "fidget-spinner",
        description: "Base skill for working inside a Fidget Spinner project through the local DAG and MCP surface.",
        resource_uri: "fidget-spinner://skill/fidget-spinner",
        body: include_str!("../../../assets/codex-skills/fidget-spinner/SKILL.md"),
    },
    BundledSkill {
        name: "frontier-loop",
        description: "Aggressive autonomous frontier-push specialization for Fidget Spinner.",
        resource_uri: "fidget-spinner://skill/frontier-loop",
        body: include_str!("../../../assets/codex-skills/frontier-loop/SKILL.md"),
    },
];

#[must_use]
pub(crate) const fn default_bundled_skill() -> BundledSkill {
    BUNDLED_SKILLS[0]
}

#[must_use]
pub(crate) const fn frontier_loop_bundled_skill() -> BundledSkill {
    BUNDLED_SKILLS[1]
}

#[must_use]
pub(crate) fn bundled_skill(name: &str) -> Option<BundledSkill> {
    BUNDLED_SKILLS
        .iter()
        .copied()
        .find(|skill| skill.name == name)
}

#[must_use]
pub(crate) fn bundled_skill_summaries() -> Vec<BundledSkillSummary> {
    BUNDLED_SKILLS
        .iter()
        .copied()
        .map(BundledSkill::summary)
        .collect()
}
