use rusqlite::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub uid: String,
    pub index: i32,
    pub summary: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub attachments: Vec<String>,
}

impl Task {
    pub fn from_row(row: &Row) -> Result<Self, rusqlite::Error> {
        let categories_json: Option<String> = row.get(7)?;
        let tags = categories_json
            .and_then(|json| serde_json::from_str::<Vec<String>>(&json).ok())
            .unwrap_or_default();

        let x_props_json: Option<String> = row.get(8)?;
        let project = x_props_json
            .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok())
            .and_then(|v| v.get("X-PROJECT")?.as_str().map(|s| s.to_string()));

        let attachments_json: Option<String> = row.get(10)?;
        let attachments = attachments_json
            .and_then(|json| serde_json::from_str::<Vec<String>>(&json).ok())
            .unwrap_or_default();

        Ok(Task {
            uid: row.get(0)?,
            index: row.get(1)?,
            summary: row.get(2)?,
            status: row.get(3)?,
            due: row.get(4)?,
            wait: row.get(5)?,
            priority: row.get(6)?,
            tags,
            project,
            url: row.get(9)?,
            attachments,
        })
    }
}
