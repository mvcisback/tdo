use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::task::Task;

#[derive(Debug, Serialize)]
pub struct MutationResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<Task>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tasks: Option<Vec<Task>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct TaskInput {
    pub summary: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub due: Option<String>,
    #[serde(default)]
    pub wait: Option<String>,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub url: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct TaskChanges {
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub due: Option<String>,
    #[serde(default)]
    pub wait: Option<String>,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub add_tags: Option<Vec<String>>,
    #[serde(default)]
    pub remove_tags: Option<Vec<String>>,
    #[serde(default)]
    pub url: Option<String>,
}

fn now_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

fn next_available_index(conn: &Connection) -> Result<i32, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "SELECT task_index FROM tasks WHERE task_index IS NOT NULL ORDER BY task_index"
    )?;
    let indices: Vec<i32> = stmt
        .query_map([], |row| row.get(0))?
        .filter_map(|r| r.ok())
        .collect();

    if indices.is_empty() {
        return Ok(1);
    }

    // Find first hole
    let mut expected = 1;
    for idx in &indices {
        if *idx > expected {
            return Ok(expected);
        }
        expected = idx + 1;
    }

    // No holes, return max + 1
    Ok(indices.last().unwrap() + 1)
}

pub fn add_task(conn: &Connection, input: &TaskInput) -> Result<MutationResult, Box<dyn std::error::Error>> {
    let uid = Uuid::new_v4().to_string();
    let now = now_timestamp();
    let index = next_available_index(conn)?;

    let status = input.status.as_deref().unwrap_or("NEEDS-ACTION");
    let x_properties = if let Some(ref project) = input.project {
        serde_json::json!({"X-PROJECT": project}).to_string()
    } else {
        "{}".to_string()
    };
    let categories = serde_json::to_string(&input.tags.as_ref().unwrap_or(&vec![]))?;

    // Parse due/wait to get UTC timestamps
    let due_utc = input.due.as_ref().and_then(|d| parse_datetime_to_timestamp(d));
    let wait_utc = input.wait.as_ref().and_then(|w| parse_datetime_to_timestamp(w));

    conn.execute(
        "INSERT INTO tasks (
            uid, summary, status, due, wait, due_utc, wait_utc, priority,
            x_properties, categories, url, attachments, href,
            pending_action, last_synced, updated_at, task_index
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            uid,
            input.summary,
            status,
            input.due,
            input.wait,
            due_utc,
            wait_utc,
            input.priority,
            x_properties,
            categories,
            input.url,
            "[]",  // attachments
            Option::<String>::None,  // href
            "create",  // pending_action
            Option::<f64>::None,  // last_synced
            now,
            index,
        ],
    )?;

    let task = get_task_by_uid(conn, &uid)?;

    Ok(MutationResult {
        success: true,
        task,
        tasks: None,
        error: None,
        index: Some(index),
    })
}

pub fn modify_tasks(
    conn: &Connection,
    indices: &[i32],
    changes: &TaskChanges,
) -> Result<MutationResult, Box<dyn std::error::Error>> {
    let mut modified_tasks = Vec::new();

    for &index in indices {
        if let Some(task) = modify_single_task(conn, index, changes)? {
            modified_tasks.push(task);
        }
    }

    Ok(MutationResult {
        success: true,
        task: None,
        tasks: Some(modified_tasks),
        error: None,
        index: None,
    })
}

fn modify_single_task(
    conn: &Connection,
    index: i32,
    changes: &TaskChanges,
) -> Result<Option<Task>, Box<dyn std::error::Error>> {
    // Get current task
    let mut stmt = conn.prepare(
        "SELECT uid, summary, status, due, wait, priority, x_properties, categories, url, pending_action
         FROM tasks WHERE task_index = ?"
    )?;

    let row = stmt.query_row([index], |row| {
        Ok((
            row.get::<_, String>(0)?,    // uid
            row.get::<_, String>(1)?,    // summary
            row.get::<_, String>(2)?,    // status
            row.get::<_, Option<String>>(3)?,  // due
            row.get::<_, Option<String>>(4)?,  // wait
            row.get::<_, Option<i32>>(5)?,     // priority
            row.get::<_, Option<String>>(6)?,  // x_properties
            row.get::<_, Option<String>>(7)?,  // categories
            row.get::<_, Option<String>>(8)?,  // url
            row.get::<_, Option<String>>(9)?,  // pending_action
        ))
    });

    let (uid, mut summary, mut status, mut due, mut wait, mut priority, x_props_str, cats_str, mut url, pending_action) = match row {
        Ok(r) => r,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    // Apply changes
    if let Some(ref s) = changes.summary {
        summary = s.clone();
    }
    if let Some(ref s) = changes.status {
        status = s.clone();
    }
    if let Some(ref d) = changes.due {
        due = Some(d.clone());
    }
    if let Some(ref w) = changes.wait {
        wait = Some(w.clone());
    }
    if changes.priority.is_some() {
        priority = changes.priority;
    }
    if let Some(ref u) = changes.url {
        url = Some(u.clone());
    }

    // Handle x_properties (project)
    let mut x_props: serde_json::Value = x_props_str
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(serde_json::json!({}));

    if let Some(ref project) = changes.project {
        x_props["X-PROJECT"] = serde_json::Value::String(project.clone());
    }

    // Handle categories (tags)
    let mut cats: Vec<String> = cats_str
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();

    if let Some(ref add_tags) = changes.add_tags {
        for tag in add_tags {
            if !cats.contains(tag) {
                cats.push(tag.clone());
            }
        }
    }
    if let Some(ref remove_tags) = changes.remove_tags {
        cats.retain(|t| !remove_tags.contains(t));
    }

    let now = now_timestamp();
    let due_utc = due.as_ref().and_then(|d| parse_datetime_to_timestamp(d));
    let wait_utc = wait.as_ref().and_then(|w| parse_datetime_to_timestamp(w));

    // Determine pending_action
    let new_pending = if pending_action.as_deref() == Some("create") {
        "create"
    } else {
        "update"
    };

    conn.execute(
        "UPDATE tasks SET
            summary = ?, status = ?, due = ?, wait = ?, due_utc = ?, wait_utc = ?,
            priority = ?, x_properties = ?, categories = ?, url = ?,
            pending_action = ?, updated_at = ?
         WHERE uid = ?",
        params![
            summary,
            status,
            due,
            wait,
            due_utc,
            wait_utc,
            priority,
            x_props.to_string(),
            serde_json::to_string(&cats)?,
            url,
            new_pending,
            now,
            uid,
        ],
    )?;

    get_task_by_uid(conn, &uid)
}

pub fn complete_tasks(conn: &Connection, indices: &[i32]) -> Result<MutationResult, Box<dyn std::error::Error>> {
    let mut completed = Vec::new();

    for &index in indices {
        if let Some(task) = complete_single_task(conn, index)? {
            completed.push(task);
        }
    }

    Ok(MutationResult {
        success: true,
        task: None,
        tasks: Some(completed),
        error: None,
        index: None,
    })
}

fn complete_single_task(conn: &Connection, index: i32) -> Result<Option<Task>, Box<dyn std::error::Error>> {
    // Get task from active table
    let mut stmt = conn.prepare("SELECT * FROM tasks WHERE task_index = ?")?;
    let task = stmt.query_row([index], |row| Task::from_row(row));

    let task = match task {
        Ok(t) => t?,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let now = now_timestamp();

    // Get pending_action to determine new pending_action
    let pending: Option<String> = conn.query_row(
        "SELECT pending_action FROM tasks WHERE task_index = ?",
        [index],
        |row| row.get(0),
    )?;

    let new_pending = if pending.as_deref() == Some("create") { "create" } else { "update" };

    // Insert into completed_tasks
    conn.execute(
        "INSERT INTO completed_tasks (
            uid, summary, status, due, wait, due_utc, wait_utc, priority,
            x_properties, categories, url, attachments, href,
            pending_action, last_synced, updated_at, completed_at, task_index
        ) SELECT
            uid, summary, 'COMPLETED', due, wait, due_utc, wait_utc, priority,
            x_properties, categories, url, attachments, href,
            ?, last_synced, ?, ?, task_index
        FROM tasks WHERE task_index = ?",
        params![new_pending, now, now, index],
    )?;

    // Delete from active tasks
    conn.execute("DELETE FROM tasks WHERE task_index = ?", [index])?;

    Ok(Some(task))
}

pub fn start_tasks(conn: &Connection, indices: &[i32]) -> Result<MutationResult, Box<dyn std::error::Error>> {
    set_status(conn, indices, "IN-PROCESS")
}

pub fn stop_tasks(conn: &Connection, indices: &[i32]) -> Result<MutationResult, Box<dyn std::error::Error>> {
    set_status(conn, indices, "NEEDS-ACTION")
}

fn set_status(conn: &Connection, indices: &[i32], status: &str) -> Result<MutationResult, Box<dyn std::error::Error>> {
    let changes = TaskChanges {
        status: Some(status.to_string()),
        ..Default::default()
    };
    modify_tasks(conn, indices, &changes)
}

pub fn delete_tasks(conn: &Connection, indices: &[i32]) -> Result<MutationResult, Box<dyn std::error::Error>> {
    let mut deleted = Vec::new();

    for &index in indices {
        if let Some(task) = delete_single_task(conn, index)? {
            deleted.push(task);
        }
    }

    Ok(MutationResult {
        success: true,
        task: None,
        tasks: Some(deleted),
        error: None,
        index: None,
    })
}

fn delete_single_task(conn: &Connection, index: i32) -> Result<Option<Task>, Box<dyn std::error::Error>> {
    // Get task
    let mut stmt = conn.prepare("SELECT * FROM tasks WHERE task_index = ?")?;
    let task = stmt.query_row([index], |row| Task::from_row(row));

    let task = match task {
        Ok(t) => t?,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let now = now_timestamp();

    // Check pending_action
    let pending: Option<String> = conn.query_row(
        "SELECT pending_action FROM tasks WHERE task_index = ?",
        [index],
        |row| row.get(0),
    )?;

    // If never synced, just delete
    if pending.as_deref() == Some("create") {
        conn.execute("DELETE FROM tasks WHERE task_index = ?", [index])?;
        return Ok(Some(task));
    }

    // Move to deleted_tasks
    conn.execute(
        "INSERT INTO deleted_tasks (
            uid, summary, status, due, wait, due_utc, wait_utc, priority,
            x_properties, categories, url, attachments, href,
            last_synced, deleted_at, task_index
        ) SELECT
            uid, summary, status, due, wait, due_utc, wait_utc, priority,
            x_properties, categories, url, attachments, href,
            last_synced, ?, task_index
        FROM tasks WHERE task_index = ?",
        params![now, index],
    )?;

    conn.execute("DELETE FROM tasks WHERE task_index = ?", [index])?;

    Ok(Some(task))
}

fn get_task_by_uid(conn: &Connection, uid: &str) -> Result<Option<Task>, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare(
        "SELECT uid, task_index, summary, status, due, wait, priority, categories, x_properties, url, attachments
         FROM tasks WHERE uid = ?"
    )?;

    let result = stmt.query_row([uid], |row| Task::from_row(row));

    match result {
        Ok(t) => Ok(Some(t?)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn parse_datetime_to_timestamp(s: &str) -> Option<f64> {
    // Try ISO format first
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp() as f64);
    }
    // Try naive datetime
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc().timestamp() as f64);
    }
    // Try date only
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(d.and_hms_opt(0, 0, 0)?.and_utc().timestamp() as f64);
    }
    None
}
