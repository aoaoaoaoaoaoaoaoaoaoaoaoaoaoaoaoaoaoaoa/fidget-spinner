use crate::{Id, MetricKey, RegimeNumber};
use serde_json::{Map, Number, Value};
use std::{collections::BTreeMap, fmt::Write as _};

/// The record kinds a reference can name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RefKind {
    Trial,
    Iset,
    Baseline,
    Metric,
}

/// A resolved reference inside data.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Ref {
    Trial(Id),
    Iset(Id),
    Baseline(Id),
    Metric(RegimeNumber, MetricKey),
}

/// Frontier payload: JSON whose reference atoms are resolved to incarnations.
#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    Null,
    Bool(bool),
    Number(Number),
    Text(String),
    List(Vec<Data>),
    Map(BTreeMap<String, Data>),
    Ref(Ref),
}

pub type Path = Vec<String>;
pub type Fields = BTreeMap<String, Data>;

/// A refusal of written data, naming where it occurred.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataError {
    pub path: Path,
    pub message: String,
}

pub type Resolver<'a> = dyn FnMut(RefKind, &str) -> Result<Ref, String> + 'a;
pub type Namer<'a> = dyn Fn(&Ref) -> Option<String> + 'a;

impl RefKind {
    pub const ALL: [Self; 4] = [Self::Trial, Self::Iset, Self::Baseline, Self::Metric];
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trial => "trial",
            Self::Iset => "iset",
            Self::Baseline => "baseline",
            Self::Metric => "metric",
        }
    }
    pub fn parse(raw: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == raw)
    }
}

impl Ref {
    pub const fn kind(&self) -> RefKind {
        match self {
            Self::Trial(_) => RefKind::Trial,
            Self::Iset(_) => RefKind::Iset,
            Self::Baseline(_) => RefKind::Baseline,
            Self::Metric(..) => RefKind::Metric,
        }
    }
    pub const fn id(&self) -> Option<Id> {
        match self {
            Self::Trial(id) | Self::Iset(id) | Self::Baseline(id) => Some(*id),
            Self::Metric(..) => None,
        }
    }
}

pub fn join(path: &[String]) -> String {
    path.join(".")
}

impl DataError {
    fn at(path: &[String], message: impl Into<String>) -> Self {
        Self {
            path: path.to_vec(),
            message: message.into(),
        }
    }
}

impl Data {
    pub const fn empty() -> Self {
        Self::Map(BTreeMap::new())
    }

    /// Read written JSON, resolving every reference atom.
    pub(crate) fn parse(value: &Value, resolve: &mut Resolver<'_>) -> Result<Self, DataError> {
        Self::read(value, resolve, &mut Vec::new())
    }

    fn read(value: &Value, resolve: &mut Resolver<'_>, path: &mut Path) -> Result<Self, DataError> {
        Ok(match value {
            Value::Null => Self::Null,
            Value::Bool(b) => Self::Bool(*b),
            Value::Number(n) => Self::Number(n.clone()),
            Value::String(s) => Self::Text(s.clone()),
            Value::Array(items) => {
                let mut list = Vec::with_capacity(items.len());
                for (index, item) in items.iter().enumerate() {
                    path.push(index.to_string());
                    list.push(Self::read(item, resolve, path)?);
                    path.pop();
                }
                Self::List(list)
            }
            Value::Object(map) if map.contains_key("$fs") => {
                Self::Ref(reference(map, resolve).map_err(|message| DataError::at(path, message))?)
            }
            Value::Object(map) => {
                let mut fields = BTreeMap::new();
                for (key, item) in map {
                    path.push(key.clone());
                    fields.insert(key.clone(), Self::read(item, resolve, path)?);
                    path.pop();
                }
                Self::Map(fields)
            }
        })
    }

    pub fn json(&self, name: &Namer<'_>) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(b) => Value::Bool(*b),
            Self::Number(n) => Value::Number(n.clone()),
            Self::Text(t) => Value::String(t.clone()),
            Self::List(items) => Value::Array(items.iter().map(|d| d.json(name)).collect()),
            Self::Map(fields) => Value::Object(
                fields
                    .iter()
                    .map(|(k, d)| (k.clone(), d.json(name)))
                    .collect(),
            ),
            Self::Ref(r) => {
                let mut atom = Map::new();
                atom.insert("$fs".into(), "ref".into());
                atom.insert("kind".into(), r.kind().as_str().into());
                match name(r) {
                    Some(id) => atom.insert("id".into(), id.into()),
                    None => atom.insert("removed".into(), true.into()),
                };
                Value::Object(atom)
            }
        }
    }

    /// Compact JSON with references written `@kind:name`, or `@kind:-` once removed.
    pub fn compact(&self, name: &Namer<'_>) -> String {
        let mut out = String::new();
        self.write(&mut out, name);
        out
    }

    fn write(&self, out: &mut String, name: &Namer<'_>) {
        match self {
            Self::Null => out.push_str("null"),
            Self::Bool(b) => {
                let _ = write!(out, "{b}");
            }
            Self::Number(n) => {
                let _ = write!(out, "{n}");
            }
            Self::Text(t) => out.push_str(&Value::String(t.clone()).to_string()),
            Self::List(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write(out, name);
                }
                out.push(']');
            }
            Self::Map(fields) => {
                out.push('{');
                for (i, (key, item)) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&Value::String(key.clone()).to_string());
                    out.push(':');
                    item.write(out, name);
                }
                out.push('}');
            }
            Self::Ref(r) => {
                let _ = write!(
                    out,
                    "@{}:{}",
                    r.kind().as_str(),
                    name(r).as_deref().unwrap_or("-")
                );
            }
        }
    }

    pub const fn fields(&self) -> Option<&Fields> {
        match self {
            Self::Map(fields) => Some(fields),
            _ => None,
        }
    }

    /// Apply simultaneous path edits checked against the value before them.
    pub(crate) fn edit(
        &mut self,
        set: Vec<(Path, Self)>,
        delete: &[Path],
    ) -> Result<(), DataError> {
        let paths: Vec<&Path> = set.iter().map(|(p, _)| p).chain(delete).collect();
        for (i, path) in paths.iter().enumerate() {
            if path.is_empty() {
                return Err(DataError::at(path, "the empty path is refused"));
            }
            if paths[..i]
                .iter()
                .any(|other| other.starts_with(path) || path.starts_with(other))
            {
                return Err(DataError::at(path, "overlaps another path of this edit"));
            }
        }
        for (path, _) in &set {
            self.parent(path)?;
        }
        for path in delete {
            let (parent, key) = self.parent(path)?;
            if !parent.contains_key(key) {
                return Err(DataError::at(path, "no such member"));
            }
        }
        for (path, value) in set {
            let (parent, key) = self.parent_mut(&path)?;
            parent.insert(key, value);
        }
        for path in delete {
            let (parent, key) = self.parent_mut(path)?;
            parent.remove(&key);
        }
        Ok(())
    }

    fn parent<'a>(&self, path: &'a Path) -> Result<(&Fields, &'a String), DataError> {
        let (last, init) = path
            .split_last()
            .ok_or_else(|| DataError::at(path, "the empty path is refused"))?;
        let mut node = self;
        for (depth, key) in init.iter().enumerate() {
            node = node
                .fields()
                .and_then(|fields| fields.get(key))
                .ok_or_else(|| DataError::at(&path[..=depth], "no such object"))?;
        }
        let fields = node
            .fields()
            .ok_or_else(|| DataError::at(init, "is not an object"))?;
        Ok((fields, last))
    }

    fn parent_mut(&mut self, path: &Path) -> Result<(&mut Fields, String), DataError> {
        let (last, init) = path
            .split_last()
            .ok_or_else(|| DataError::at(path, "the empty path is refused"))?;
        let mut node = self;
        for key in init {
            node = match node {
                Self::Map(fields) => fields
                    .get_mut(key)
                    .ok_or_else(|| DataError::at(path, "no such object"))?,
                _ => return Err(DataError::at(path, "is not an object")),
            };
        }
        match node {
            Self::Map(fields) => Ok((fields, last.clone())),
            _ => Err(DataError::at(init, "is not an object")),
        }
    }

    pub fn refs(&self, visit: &mut dyn FnMut(&Ref)) {
        match self {
            Self::List(items) => items.iter().for_each(|d| d.refs(visit)),
            Self::Map(fields) => fields.values().for_each(|d| d.refs(visit)),
            Self::Ref(r) => visit(r),
            _ => {}
        }
    }

    /// The first field path or text containing a lowercase query, with that text.
    pub fn find(&self, query: &str) -> Option<(String, String)> {
        let mut path = Vec::new();
        self.seek(query, &mut path)
    }

    fn seek(&self, query: &str, path: &mut Path) -> Option<(String, String)> {
        match self {
            Self::Text(text) if text.to_lowercase().contains(query) => {
                Some((join(path), text.clone()))
            }
            Self::List(items) => items.iter().enumerate().find_map(|(i, d)| {
                path.push(i.to_string());
                let found = d.seek(query, path);
                path.pop();
                found
            }),
            Self::Map(fields) => fields.iter().find_map(|(key, d)| {
                path.push(key.clone());
                let found = if join(path).to_lowercase().contains(query) {
                    Some((join(path), String::new()))
                } else {
                    d.seek(query, path)
                };
                path.pop();
                found
            }),
            _ => None,
        }
    }

    /// The text of a top-level field, for headlines and badges.
    pub fn text(&self, field: &str) -> Option<String> {
        match self.fields()?.get(field)? {
            Self::Text(text) => Some(text.clone()),
            Self::Number(n) => Some(n.to_string()),
            Self::Bool(b) => Some(b.to_string()),
            _ => None,
        }
    }
}

fn reference(map: &Map<String, Value>, resolve: &mut Resolver<'_>) -> Result<Ref, String> {
    let shape = || r#"$fs is reserved for references {"$fs":"ref","kind":K,"id":N}"#.to_owned();
    if map.len() != 3 || map.get("$fs").and_then(Value::as_str) != Some("ref") {
        return Err(shape());
    }
    let (Some(Value::String(kind)), Some(Value::String(id))) = (map.get("kind"), map.get("id"))
    else {
        return Err(shape());
    };
    let kind = RefKind::parse(kind).ok_or_else(|| {
        format!("unknown reference kind {kind:?}; kinds: trial, iset, baseline, metric")
    })?;
    resolve(kind, id)
}
