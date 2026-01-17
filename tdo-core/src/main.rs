mod db;
mod mutations;
mod task;

use std::env;
use std::io::{self, Write};

use mutations::{MutationResult, TaskChanges, TaskInput};

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
        _ => {
            return Err(format!("Unknown command: {}", cmd_type).into());
        }
    }

    Ok(())
}
