use crate::{
    SAFE_INTEGER,
    data::{Data, Path, RefKind, join},
};
use serde_json::{Map, Number, Value};
use std::collections::{BTreeMap, BTreeSet};

/// The two kinds of work a definition can govern.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Carrier {
    Trial,
    Iset,
}

/// A data slot of work that a definition governs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Slot {
    Data,
    Answer,
    Plan,
    Report,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub shape: Shape,
    pub title: Option<String>,
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Shape {
    Iset { data: Fields, answer: Fields },
    Trial { plan: Fields, report: Fields },
}

pub type Fields = BTreeMap<String, Field>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub ty: Type,
    pub required: bool,
    pub title: Option<String>,
    pub description: Option<String>,
    pub show: Option<Show>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Show {
    Headline,
    Badge,
    Card,
    Section,
    Hidden,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    Bool,
    Text,
    Int,
    Float,
    Enum(Vec<String>),
    List(Box<Type>),
    Map(Box<Type>),
    Record(Fields),
    Variant(BTreeMap<String, Fields>),
    Ref(RefKind),
}

impl Carrier {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trial => "trial",
            Self::Iset => "iset",
        }
    }
}

impl Slot {
    pub const fn carrier(self) -> Carrier {
        match self {
            Self::Data | Self::Answer => Carrier::Iset,
            Self::Plan | Self::Report => Carrier::Trial,
        }
    }
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Answer => "answer",
            Self::Plan => "plan",
            Self::Report => "report",
        }
    }
}

const SHOWS: [(&str, Show); 5] = [
    ("headline", Show::Headline),
    ("badge", Show::Badge),
    ("card", Show::Card),
    ("section", Show::Section),
    ("hidden", Show::Hidden),
];
const SCALARS: [(&str, Type); 4] = [
    ("bool", Type::Bool),
    ("text", Type::Text),
    ("int", Type::Int),
    ("float", Type::Float),
];

impl Schema {
    pub const fn carrier(&self) -> Carrier {
        match self.shape {
            Shape::Iset { .. } => Carrier::Iset,
            Shape::Trial { .. } => Carrier::Trial,
        }
    }

    pub const fn fields(&self, slot: Slot) -> Option<&Fields> {
        match (&self.shape, slot) {
            (Shape::Iset { data, .. }, Slot::Data) => Some(data),
            (Shape::Iset { answer, .. }, Slot::Answer) => Some(answer),
            (Shape::Trial { plan, .. }, Slot::Plan) => Some(plan),
            (Shape::Trial { report, .. }, Slot::Report) => Some(report),
            _ => None,
        }
    }

    /// Fields of the work's own slot (data or plan) shown with a display hint.
    pub fn shown(&self, show: Show) -> Vec<&str> {
        let slot = match self.shape {
            Shape::Iset { .. } => Slot::Data,
            Shape::Trial { .. } => Slot::Plan,
        };
        self.fields(slot)
            .into_iter()
            .flatten()
            .filter(|(_, field)| field.show == Some(show))
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Violations of a slot's fields, each `path: problem`.
    pub fn check(&self, slot: Slot, data: &Data) -> Vec<String> {
        let Some(fields) = self.fields(slot) else {
            return vec![format!(
                "{}: the definition governs {}s",
                slot.as_str(),
                self.carrier().as_str()
            )];
        };
        let mut out = Vec::new();
        check_fields(fields, data, &mut vec![slot.as_str().to_owned()], &mut out);
        out
    }

    pub fn parse(value: &Value) -> Result<Self, String> {
        let map = object(value, "definition")?;
        only(
            map,
            &[
                "carrier",
                "data",
                "answer",
                "plan",
                "report",
                "title",
                "description",
            ],
            "definition",
        )?;
        let pair = |a: &str, b: &str, other: [&str; 2]| -> Result<(Fields, Fields), String> {
            if let Some(key) = other.iter().find(|key| map.contains_key(**key)) {
                return Err(format!("definition.{key}: not a field of this carrier"));
            }
            Ok((
                fields(need(map, a, "definition")?, &format!("definition.{a}"))?,
                fields(need(map, b, "definition")?, &format!("definition.{b}"))?,
            ))
        };
        let shape = match map.get("carrier").and_then(Value::as_str) {
            Some("iset") => {
                let (data, answer) = pair("data", "answer", ["plan", "report"])?;
                Shape::Iset { data, answer }
            }
            Some("trial") => {
                let (plan, report) = pair("plan", "report", ["data", "answer"])?;
                Shape::Trial { plan, report }
            }
            _ => return Err(r#"definition.carrier: expected "iset" or "trial""#.into()),
        };
        Ok(Self {
            shape,
            title: text(map, "title", "definition")?,
            description: text(map, "description", "definition")?,
        })
    }

    pub fn json(&self) -> Value {
        let mut map = Map::new();
        let (carrier, slots) = match &self.shape {
            Shape::Iset { data, answer } => ("iset", [("data", data), ("answer", answer)]),
            Shape::Trial { plan, report } => ("trial", [("plan", plan), ("report", report)]),
        };
        map.insert("carrier".into(), carrier.into());
        for (key, fields) in slots {
            map.insert(key.into(), fields_json(fields));
        }
        if let Some(title) = &self.title {
            map.insert("title".into(), title.clone().into());
        }
        if let Some(description) = &self.description {
            map.insert("description".into(), description.clone().into());
        }
        Value::Object(map)
    }
}

impl Type {
    fn describe(&self) -> String {
        match self {
            Self::Bool => "bool".into(),
            Self::Text => "text".into(),
            Self::Int => "an integer within ±(2⁵³−1)".into(),
            Self::Float => "a number".into(),
            Self::Enum(options) => format!("one of {}", options.join(", ")),
            Self::List(_) => "a list".into(),
            Self::Map(_) | Self::Record(_) => "an object".into(),
            Self::Variant(tags) => format!(
                "{{\"tag\", \"value\"}} with tag one of {}",
                tags.keys().cloned().collect::<Vec<_>>().join(", ")
            ),
            Self::Ref(kind) => format!("a {} reference", kind.as_str()),
        }
    }

    fn json(&self) -> Value {
        let one = |key: &str, value: Value| {
            let mut map = Map::new();
            map.insert(key.into(), value);
            Value::Object(map)
        };
        match self {
            Self::Bool => "bool".into(),
            Self::Text => "text".into(),
            Self::Int => "int".into(),
            Self::Float => "float".into(),
            Self::Enum(options) => one("enum", options.clone().into()),
            Self::List(item) => one("list", item.json()),
            Self::Map(item) => one("map", item.json()),
            Self::Record(fields) => one("record", fields_json(fields)),
            Self::Variant(tags) => one(
                "variant",
                Value::Object(
                    tags.iter()
                        .map(|(tag, fields)| (tag.clone(), fields_json(fields)))
                        .collect(),
                ),
            ),
            Self::Ref(kind) => one("ref", kind.as_str().into()),
        }
    }
}

fn fields_json(fields: &Fields) -> Value {
    Value::Object(
        fields
            .iter()
            .map(|(name, field)| {
                let mut map = Map::new();
                map.insert("type".into(), field.ty.json());
                map.insert(
                    "presence".into(),
                    if field.required {
                        "required"
                    } else {
                        "optional"
                    }
                    .into(),
                );
                if let Some(title) = &field.title {
                    map.insert("title".into(), title.clone().into());
                }
                if let Some(description) = &field.description {
                    map.insert("description".into(), description.clone().into());
                }
                if let Some(show) = field.show {
                    let (name, _) = SHOWS
                        .iter()
                        .find(|(_, s)| *s == show)
                        .copied()
                        .unwrap_or(("hidden", Show::Hidden));
                    map.insert("show".into(), name.into());
                }
                (name.clone(), Value::Object(map))
            })
            .collect(),
    )
}

fn object<'a>(value: &'a Value, path: &str) -> Result<&'a Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{path}: expected an object"))
}

fn only(map: &Map<String, Value>, keys: &[&str], path: &str) -> Result<(), String> {
    match map.keys().find(|key| !keys.contains(&key.as_str())) {
        Some(key) => Err(format!(
            "{path}.{key}: unknown key; keys: {}",
            keys.join(", ")
        )),
        None => Ok(()),
    }
}

fn need<'a>(map: &'a Map<String, Value>, key: &str, path: &str) -> Result<&'a Value, String> {
    map.get(key).ok_or_else(|| format!("{path}.{key}: missing"))
}

fn text(map: &Map<String, Value>, key: &str, path: &str) -> Result<Option<String>, String> {
    map.get(key)
        .map(|value| {
            value
                .as_str()
                .filter(|text| !text.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| format!("{path}.{key}: expected nonempty text"))
        })
        .transpose()
}

fn fields(value: &Value, path: &str) -> Result<Fields, String> {
    object(value, path)?
        .iter()
        .map(|(name, value)| {
            if name.is_empty() || name == "$fs" {
                Err(format!("{path}: invalid field name {name:?}"))
            } else {
                Ok((name.clone(), field(value, &format!("{path}.{name}"))?))
            }
        })
        .collect()
}

fn field(value: &Value, path: &str) -> Result<Field, String> {
    let map = object(value, path)?;
    only(
        map,
        &["type", "presence", "title", "description", "show"],
        path,
    )?;
    let ty = ty(need(map, "type", path)?, &format!("{path}.type"))?;
    let required = match map.get("presence").and_then(Value::as_str) {
        Some("required") => true,
        Some("optional") => false,
        _ => {
            return Err(format!(
                r#"{path}.presence: expected "required" or "optional""#
            ));
        }
    };
    let show = map
        .get("show")
        .map(|value| {
            SHOWS
                .iter()
                .find(|(name, _)| value.as_str() == Some(name))
                .map(|(_, show)| *show)
                .ok_or_else(|| {
                    format!("{path}.show: expected one of headline, badge, card, section, hidden")
                })
        })
        .transpose()?;
    Ok(Field {
        ty,
        required,
        title: text(map, "title", path)?,
        description: text(map, "description", path)?,
        show,
    })
}

fn ty(value: &Value, path: &str) -> Result<Type, String> {
    let names = "types: bool, text, int, float, enum, list, map, record, variant, ref";
    if let Some(name) = value.as_str() {
        return SCALARS
            .iter()
            .find(|(scalar, _)| *scalar == name)
            .map(|(_, ty)| ty.clone())
            .ok_or_else(|| format!("{path}: unknown type {name:?}; {names}"));
    }
    let Some((key, inner)) = value
        .as_object()
        .filter(|map| map.len() == 1)
        .and_then(|map| map.iter().next())
    else {
        return Err(format!(
            "{path}: expected a type name or a one-key object; {names}"
        ));
    };
    let path = format!("{path}.{key}");
    Ok(match key.as_str() {
        "enum" => {
            let options: Vec<String> = inner
                .as_array()
                .into_iter()
                .flatten()
                .map(|item| item.as_str().filter(|t| !t.is_empty()).map(str::to_owned))
                .collect::<Option<_>>()
                .filter(|options: &Vec<String>| !options.is_empty())
                .ok_or_else(|| format!("{path}: expected a nonempty list of nonempty texts"))?;
            if options.iter().collect::<BTreeSet<_>>().len() != options.len() {
                return Err(format!("{path}: texts must be distinct"));
            }
            Type::Enum(options)
        }
        "list" => Type::List(Box::new(ty(inner, &path)?)),
        "map" => Type::Map(Box::new(ty(inner, &path)?)),
        "record" => Type::Record(fields(inner, &path)?),
        "variant" => {
            let tags = object(inner, &path)?;
            if tags.is_empty() {
                return Err(format!("{path}: needs a tag"));
            }
            Type::Variant(
                tags.iter()
                    .map(|(tag, f)| Ok((tag.clone(), fields(f, &format!("{path}.{tag}"))?)))
                    .collect::<Result<_, String>>()?,
            )
        }
        "ref" => Type::Ref(
            inner
                .as_str()
                .and_then(RefKind::parse)
                .ok_or_else(|| format!("{path}: expected trial, iset, baseline or metric"))?,
        ),
        _ => return Err(format!("{path}: unknown type; {names}")),
    })
}

fn check_fields(fields: &Fields, data: &Data, path: &mut Path, out: &mut Vec<String>) {
    let Data::Map(map) = data else {
        out.push(format!("{}: expected an object", join(path)));
        return;
    };
    for (name, field) in fields {
        match map.get(name) {
            None if field.required => {
                out.push(format!("{}.{name}: missing required field", join(path)));
            }
            None => {}
            Some(value) => {
                path.push(name.clone());
                check_type(&field.ty, value, path, out);
                path.pop();
            }
        }
    }
    for key in map.keys().filter(|key| !fields.contains_key(*key)) {
        out.push(format!("{}.{key}: not in the definition", join(path)));
    }
}

fn check_type(ty: &Type, value: &Data, path: &mut Path, out: &mut Vec<String>) {
    let ok = match (ty, value) {
        (Type::Bool, Data::Bool(_))
        | (Type::Text, Data::Text(_))
        | (Type::Float, Data::Number(_)) => true,
        (Type::Int, Data::Number(n)) => integral(n),
        (Type::Enum(options), Data::Text(text)) => options.contains(text),
        (Type::List(item), Data::List(items)) => {
            for (i, value) in items.iter().enumerate() {
                path.push(i.to_string());
                check_type(item, value, path, out);
                path.pop();
            }
            true
        }
        (Type::Map(item), Data::Map(map)) => {
            for (key, value) in map {
                path.push(key.clone());
                check_type(item, value, path, out);
                path.pop();
            }
            true
        }
        (Type::Record(fields), Data::Map(_)) => {
            check_fields(fields, value, path, out);
            true
        }
        (Type::Variant(tags), Data::Map(map)) => {
            match (map.len(), map.get("tag"), map.get("value")) {
                (2, Some(Data::Text(tag)), Some(inner)) if tags.contains_key(tag) => {
                    path.push("value".into());
                    check_fields(&tags[tag], inner, path, out);
                    path.pop();
                    true
                }
                _ => false,
            }
        }
        (Type::Ref(kind), Data::Ref(r)) => r.kind() == *kind,
        _ => false,
    };
    if !ok {
        out.push(format!("{}: expected {}", join(path), ty.describe()));
    }
}

#[allow(
    clippy::float_cmp,
    clippy::cast_precision_loss,
    reason = "integrality is exact; 2⁵³ − 1 is exact in binary64"
)]
fn integral(n: &Number) -> bool {
    let safe = SAFE_INTEGER as f64;
    n.as_i64().map_or_else(
        || {
            n.as_u64().map_or_else(
                || {
                    n.as_f64()
                        .is_some_and(|v| v.trunc() == v && v.abs() <= safe)
                },
                |v| v as f64 <= safe,
            )
        },
        |v| v.abs() <= SAFE_INTEGER,
    )
}
