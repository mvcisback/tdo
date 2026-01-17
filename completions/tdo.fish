# Fish shell completions for tdo
# Install to ~/.config/fish/completions/tdo.fish or /usr/share/fish/vendor_completions.d/tdo.fish

# Disable file completions by default
complete -c tdo -f

# Helper function to check if we've seen a subcommand
function __tdo_needs_command
    set -l cmd (commandline -opc)
    set -l subcmds add modify do start stop del list waiting pull push sync show undo attach prioritize move config complete
    for i in $cmd[2..-1]
        if contains -- $i $subcmds
            return 1
        end
    end
    return 0
end

function __tdo_using_command
    set -l cmd (commandline -opc)
    set -l subcmds add modify do start stop del list waiting pull push sync show undo attach prioritize move config complete
    for i in $cmd[2..-1]
        if contains -- $i $subcmds
            if test "$i" = "$argv[1]"
                return 0
            end
            return 1
        end
    end
    return 1
end

# Global options
complete -c tdo -l version -d "Show version"
complete -c tdo -l env -d "Environment name" -r -a "(tdo complete envs 2>/dev/null)"

# Commands
complete -c tdo -n __tdo_needs_command -a add -d "Create new task"
complete -c tdo -n __tdo_needs_command -a modify -d "Update task(s)"
complete -c tdo -n __tdo_needs_command -a do -d "Mark task(s) as completed"
complete -c tdo -n __tdo_needs_command -a start -d "Start task (IN-PROCESS)"
complete -c tdo -n __tdo_needs_command -a stop -d "Stop task (NEEDS-ACTION)"
complete -c tdo -n __tdo_needs_command -a del -d "Delete task(s)"
complete -c tdo -n __tdo_needs_command -a list -d "Show active tasks"
complete -c tdo -n __tdo_needs_command -a waiting -d "Show waiting tasks"
complete -c tdo -n __tdo_needs_command -a pull -d "Download from CalDAV"
complete -c tdo -n __tdo_needs_command -a push -d "Upload to CalDAV"
complete -c tdo -n __tdo_needs_command -a sync -d "Pull then push"
complete -c tdo -n __tdo_needs_command -a show -d "Show task details"
complete -c tdo -n __tdo_needs_command -a undo -d "Undo last operation"
complete -c tdo -n __tdo_needs_command -a attach -d "Manage attachments"
complete -c tdo -n __tdo_needs_command -a prioritize -d "Interactive priority"
complete -c tdo -n __tdo_needs_command -a move -d "Move task to another env"
complete -c tdo -n __tdo_needs_command -a config -d "Configuration"
complete -c tdo -n __tdo_needs_command -a complete -d "Shell completion data"

# Task index filter (before command) - show task indices with descriptions
complete -c tdo -n __tdo_needs_command -a "(tdo complete tasks 2>/dev/null)"

# list command options
complete -c tdo -n "__tdo_using_command list" -l no-reverse -d "Don't reverse sort order"

# attach command options
complete -c tdo -n "__tdo_using_command attach" -l fmttype -d "MIME type" -r
complete -c tdo -n "__tdo_using_command attach" -l remove -d "Remove attachment"
complete -c tdo -n "__tdo_using_command attach" -l list -d "List attachments"

# move command - destination environment
complete -c tdo -n "__tdo_using_command move" -a "(tdo complete envs 2>/dev/null)" -d "Destination environment"

# complete command subcommands
complete -c tdo -n "__tdo_using_command complete" -a "envs" -d "List environments"
complete -c tdo -n "__tdo_using_command complete" -a "tasks" -d "List task indices"
complete -c tdo -n "__tdo_using_command complete" -a "projects" -d "List projects"
complete -c tdo -n "__tdo_using_command complete" -a "tags" -d "List tags"

# config subcommand
complete -c tdo -n "__tdo_using_command config" -a "init" -d "Initialize config"

# config init options
function __tdo_config_init
    set -l cmd (commandline -opc)
    if contains -- config $cmd
        if contains -- init $cmd
            return 0
        end
    end
    return 1
end

complete -c tdo -n __tdo_config_init -l config-home -d "Config directory" -r
complete -c tdo -n __tdo_config_init -l calendar-url -d "CalDAV URL" -r
complete -c tdo -n __tdo_config_init -l username -d "CalDAV username" -r
complete -c tdo -n __tdo_config_init -l password -d "CalDAV password" -r
complete -c tdo -n __tdo_config_init -l token -d "CalDAV token" -r
complete -c tdo -n __tdo_config_init -l force -d "Overwrite existing config"

# Token completions for add/modify commands
function __tdo_add_or_modify
    set -l cmd (commandline -opc)
    for i in $cmd[2..-1]
        if contains -- $i add modify
            return 0
        end
    end
    return 1
end

# Priority values
complete -c tdo -n __tdo_add_or_modify -a "pri:H" -d "High priority"
complete -c tdo -n __tdo_add_or_modify -a "pri:M" -d "Medium priority"
complete -c tdo -n __tdo_add_or_modify -a "pri:L" -d "Low priority"

# Common due/wait values
complete -c tdo -n __tdo_add_or_modify -a "due:today" -d "Due today"
complete -c tdo -n __tdo_add_or_modify -a "due:tomorrow" -d "Due tomorrow"
complete -c tdo -n __tdo_add_or_modify -a "due:eod" -d "End of day"
complete -c tdo -n __tdo_add_or_modify -a "due:eow" -d "End of week"
complete -c tdo -n __tdo_add_or_modify -a "due:eom" -d "End of month"
complete -c tdo -n __tdo_add_or_modify -a "due:eoq" -d "End of quarter"
complete -c tdo -n __tdo_add_or_modify -a "due:eoy" -d "End of year"
complete -c tdo -n __tdo_add_or_modify -a "due:mon" -d "Monday"
complete -c tdo -n __tdo_add_or_modify -a "due:tue" -d "Tuesday"
complete -c tdo -n __tdo_add_or_modify -a "due:wed" -d "Wednesday"
complete -c tdo -n __tdo_add_or_modify -a "due:thu" -d "Thursday"
complete -c tdo -n __tdo_add_or_modify -a "due:fri" -d "Friday"
complete -c tdo -n __tdo_add_or_modify -a "due:sat" -d "Saturday"
complete -c tdo -n __tdo_add_or_modify -a "due:sun" -d "Sunday"
complete -c tdo -n __tdo_add_or_modify -a "due:1d" -d "In 1 day"
complete -c tdo -n __tdo_add_or_modify -a "due:2d" -d "In 2 days"
complete -c tdo -n __tdo_add_or_modify -a "due:1w" -d "In 1 week"
complete -c tdo -n __tdo_add_or_modify -a "due:2w" -d "In 2 weeks"

complete -c tdo -n __tdo_add_or_modify -a "wait:tomorrow" -d "Wait until tomorrow"
complete -c tdo -n __tdo_add_or_modify -a "wait:1d" -d "Wait 1 day"
complete -c tdo -n __tdo_add_or_modify -a "wait:1w" -d "Wait 1 week"

# Status values
complete -c tdo -n __tdo_add_or_modify -a "status:NEEDS-ACTION" -d "Needs action"
complete -c tdo -n __tdo_add_or_modify -a "status:IN-PROCESS" -d "In process"
complete -c tdo -n __tdo_add_or_modify -a "status:COMPLETED" -d "Completed"

# Dynamic project completions
complete -c tdo -n __tdo_add_or_modify -a "(tdo complete projects 2>/dev/null | sed 's/^/project:/')" -d "Project"

# Dynamic tag completions (with + prefix for adding)
complete -c tdo -n __tdo_add_or_modify -a "(tdo complete tags 2>/dev/null | sed 's/^/+/')" -d "Add tag"

# Dynamic tag completions (with - prefix for removing)
complete -c tdo -n "__tdo_using_command modify" -a "(tdo complete tags 2>/dev/null | sed 's/^/-/')" -d "Remove tag"
