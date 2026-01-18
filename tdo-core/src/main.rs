mod db;
mod mutations;
mod task;

use std::env;
use std::io::{self, Write};

use mutations::{TaskChanges, TaskInput};

fn main() {
    let args: Vec<String> = env::args().collect();

    // Fast path for shell completions: tdo-core complete <type> [env]
    if args.len() >= 3 && args[1] == "complete" {
        let completion_type = &args[2];
        let env_name = args.get(3).map(|s| s.as_str()).unwrap_or("default");

        if let Err(e) = handle_complete(completion_type, env_name) {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
        return;
    }

    // JSON command mode: tdo-core '{"command": "...", ...}'
    if args.len() >= 2 {
        let json_input = &args[1];
        if let Err(e) = handle_json_command(json_input) {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
        return;
    }

    eprintln!("Usage: tdo-core complete <type> [env]");
    eprintln!("       tdo-core '<json command>'");
    std::process::exit(1);
}

fn handle_complete(completion_type: &str, env_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = db::Database::open(env_name)?;
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    match completion_type {
        "tasks" => {
            for (index, summary) in db.get_task_completions()? {
                writeln!(handle, "{}\t{}", index, summary)?;
            }
        }
        "tags" => {
            for tag in db.get_tags()? {
                writeln!(handle, "{}", tag)?;
            }
        }
        "projects" => {
            for project in db.get_projects()? {
                writeln!(handle, "{}", project)?;
            }
        }
        "envs" => {
            for env in db::list_environments()? {
                writeln!(handle, "{}", env)?;
            }
        }
        _ => {
            return Err(format!("Unknown completion type: {}", completion_type).into());
        }
    }

    Ok(())
}

fn handle_json_command(json_input: &str) -> Result<(), Box<dyn std::error::Error>> {
    let command: serde_json::Value = serde_json::from_str(json_input)?;

    let cmd_type = command.get("command")
        .and_then(|v| v.as_str())
        .ok_or("Missing 'command' field")?;

    let env_name = command.get("env")
        .and_then(|v| v.as_str())
        .unwrap_or("default");

    match cmd_type {
        "complete" => {
            let comp_type = command.get("type")
                .and_then(|v| v.as_str())
                .ok_or("Missing 'type' field for complete command")?;
            handle_complete(comp_type, env_name)?;
        }
        "list" => {
            let db = db::Database::open(env_name)?;
            let tasks = db.list_tasks()?;
            let json = serde_json::to_string(&tasks)?;
            println!("{}", json);
        }
        "show" => {
            let db = db::Database::open(env_name)?;
            let indices: Vec<i32> = command.get("indices")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let tasks = db.get_tasks_by_indices(&indices)?;
            let json = serde_json::to_string(&tasks)?;
            println!("{}", json);
        }
        "add" => {
            let db = db::Database::open(env_name)?;
            let input: TaskInput = serde_json::from_value(
                command.get("task").cloned().ok_or("Missing 'task' field")?
            )?;
            let result = mutations::add_task(db.connection(), &input)?;
            let json = serde_json::to_string(&result)?;
            println!("{}", json);
        }
        "modify" => {
            let db = db::Database::open(env_name)?;
            let indices: Vec<i32> = command.get("indices")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let changes: TaskChanges = serde_json::from_value(
                command.get("changes").cloned().unwrap_or(serde_json::json!({}))
            )?;
            let result = mutations::modify_tasks(db.connection(), &indices, &changes)?;
            let json = serde_json::to_string(&result)?;
            println!("{}", json);
        }
        "do" => {
            let db = db::Database::open(env_name)?;
            let indices: Vec<i32> = command.get("indices")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let result = mutations::complete_tasks(db.connection(), &indices)?;
            let json = serde_json::to_string(&result)?;
            println!("{}", json);
        }
        "start" => {
            let db = db::Database::open(env_name)?;
            let indices: Vec<i32> = command.get("indices")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let result = mutations::start_tasks(db.connection(), &indices)?;
            let json = serde_json::to_string(&result)?;
            println!("{}", json);
        }
        "stop" => {
            let db = db::Database::open(env_name)?;
            let indices: Vec<i32> = command.get("indices")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let result = mutations::stop_tasks(db.connection(), &indices)?;
            let json = serde_json::to_string(&result)?;
            println!("{}", json);
        }
        "delete" => {
            let db = db::Database::open(env_name)?;
            let indices: Vec<i32> = command.get("indices")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let result = mutations::delete_tasks(db.connection(), &indices)?;
            let json = serde_json::to_string(&result)?;
            println!("{}", json);
        }
        "move" => {
            let src_db = db::Database::open(env_name)?;
            let dest_env = command.get("dest_env")
                .and_then(|v| v.as_str())
                .ok_or("Missing 'dest_env' field for move command")?;
            let dest_db = db::Database::open(dest_env)?;
            let indices: Vec<i32> = command.get("indices")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let result = mutations::move_tasks(src_db.connection(), dest_db.connection(), &indices)?;
            let json = serde_json::to_string(&result)?;
            println!("{}", json);
        }
        "log_transaction" => {
            let db = db::Database::open(env_name)?;
            let diff_json = command.get("diff_json")
                .and_then(|v| v.as_str())
                .ok_or("Missing 'diff_json' field")?;
            let operation = command.get("operation")
                .and_then(|v| v.as_str())
                .ok_or("Missing 'operation' field")?;
            let max_entries = command.get("max_entries")
                .and_then(|v| v.as_i64())
                .unwrap_or(100);
            let result = mutations::log_transaction(db.connection(), diff_json, operation, max_entries)?;
            let json = serde_json::to_string(&result)?;
            println!("{}", json);
        }
        "pop_transaction" => {
            let db = db::Database::open(env_name)?;
            let result = mutations::pop_transaction(db.connection())?;
            let json = serde_json::to_string(&result)?;
            println!("{}", json);
        }
        _ => {
            return Err(format!("Unknown command: {}", cmd_type).into());
        }
    }

    Ok(())
}
