use rusqlite::{Connection, Result as SqliteResult};
use std::collections::HashSet;
use std::path::PathBuf;

use crate::task::Task;

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

impl Database {
    pub fn open(env_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let db_path = get_db_path(env_name)?;

        if !db_path.exists() {
            return Err(format!("Database not found: {:?}", db_path).into());
        }

        let conn = Connection::open(&db_path)?;
        Ok(Database { conn })
    }

    pub fn get_task_completions(&self) -> SqliteResult<Vec<(i32, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT task_index, summary FROM tasks
             WHERE status != 'COMPLETED' AND task_index IS NOT NULL
             ORDER BY task_index"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?))
        })?;

        rows.collect()
    }

    pub fn get_tags(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut stmt = self.conn.prepare(
            "SELECT categories FROM tasks WHERE status != 'COMPLETED' AND categories IS NOT NULL"
        )?;

        let mut tags = HashSet::new();

        let rows = stmt.query_map([], |row| {
            row.get::<_, String>(0)
        })?;

        for row in rows {
            if let Ok(categories_json) = row {
                if let Ok(arr) = serde_json::from_str::<Vec<String>>(&categories_json) {
                    for tag in arr {
                        tags.insert(tag);
                    }
                }
            }
        }

        let mut result: Vec<String> = tags.into_iter().collect();
        result.sort();
        Ok(result)
    }

    pub fn get_projects(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut stmt = self.conn.prepare(
            "SELECT x_properties FROM tasks WHERE status != 'COMPLETED' AND x_properties IS NOT NULL"
        )?;

        let mut projects = HashSet::new();

        let rows = stmt.query_map([], |row| {
            row.get::<_, String>(0)
        })?;

        for row in rows {
            if let Ok(props_json) = row {
                if let Ok(props) = serde_json::from_str::<serde_json::Value>(&props_json) {
                    if let Some(project) = props.get("X-PROJECT").and_then(|v| v.as_str()) {
                        projects.insert(project.to_string());
                    }
                }
            }
        }

        let mut result: Vec<String> = projects.into_iter().collect();
        result.sort();
        Ok(result)
    }

    pub fn list_tasks(&self) -> Result<Vec<Task>, Box<dyn std::error::Error>> {
        let mut stmt = self.conn.prepare(
            "SELECT uid, task_index, summary, status, due, wait, priority, categories, x_properties, url, attachments
             FROM tasks
             WHERE status != 'COMPLETED'
             ORDER BY task_index"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(Task::from_row(row))
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            tasks.push(row??);
        }
        Ok(tasks)
    }

    pub fn get_tasks_by_indices(&self, indices: &[i32]) -> Result<Vec<Task>, Box<dyn std::error::Error>> {
        if indices.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders: Vec<String> = indices.iter().map(|_| "?".to_string()).collect();
        let sql = format!(
            "SELECT uid, task_index, summary, status, due, wait, priority, categories, x_properties, url, attachments
             FROM tasks
             WHERE task_index IN ({})
             ORDER BY task_index",
            placeholders.join(", ")
        );

        let mut stmt = self.conn.prepare(&sql)?;

        let params: Vec<&dyn rusqlite::ToSql> = indices.iter()
            .map(|i| i as &dyn rusqlite::ToSql)
            .collect();

        let rows = stmt.query_map(params.as_slice(), |row| {
            Ok(Task::from_row(row))
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            tasks.push(row??);
        }
        Ok(tasks)
    }
}

fn get_db_path(env_name: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let home = dirs::home_dir().ok_or("Could not find home directory")?;
    let safe_env = env_name.replace(['/', '\\', '\0'], "_");
    Ok(home.join(".cache").join("tdo").join(&safe_env).join("tasks.db"))
}

pub fn list_environments() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let home = dirs::home_dir().ok_or("Could not find home directory")?;
    let cache_dir = home.join(".cache").join("tdo");

    if !cache_dir.exists() {
        return Ok(Vec::new());
    }

    let mut envs = Vec::new();
    for entry in std::fs::read_dir(&cache_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                envs.push(name.to_string());
            }
        }
    }
    envs.sort();
    Ok(envs)
}
