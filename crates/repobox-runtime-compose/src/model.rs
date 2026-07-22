use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct ComposeProject {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub services: BTreeMap<String, ComposeService>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct ComposeService {
    pub image: Option<String>,
    #[serde(default)]
    pub environment: ComposeEnvironment,
    #[serde(default)]
    pub depends_on: Value,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum ComposeEnvironment {
    Map(BTreeMap<String, Option<String>>),
    List(Vec<String>),
    #[default]
    Empty,
}

impl ComposeEnvironment {
    pub fn as_map(&self) -> BTreeMap<String, String> {
        match self {
            Self::Map(values) => values
                .iter()
                .filter_map(|(key, value)| value.clone().map(|value| (key.clone(), value)))
                .collect(),
            Self::List(values) => values
                .iter()
                .map(|entry| {
                    entry.split_once('=').map_or_else(
                        || (entry.clone(), String::new()),
                        |(key, value)| (key.to_owned(), value.to_owned()),
                    )
                })
                .collect(),
            Self::Empty => BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: String) {
        let mut values = self.as_map();
        values.insert(key, value);
        *self = Self::Map(
            values
                .into_iter()
                .map(|(key, value)| (key, Some(value)))
                .collect(),
        );
    }
}

impl ComposeService {
    pub fn dependencies(&self) -> Vec<String> {
        match &self.depends_on {
            Value::Array(values) => values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
            Value::Object(values) => values.keys().cloned().collect(),
            _ => vec![],
        }
    }

    pub fn remove_dependencies(&mut self, removed: &std::collections::BTreeSet<String>) {
        match &mut self.depends_on {
            Value::Array(values) => {
                values.retain(|value| value.as_str().is_none_or(|name| !removed.contains(name)));
            }
            Value::Object(values) => values.retain(|name, _| !removed.contains(name)),
            _ => {}
        }
    }
}
