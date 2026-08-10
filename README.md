# TaskCore

A lightweight, filesystem-based task queue system for Python. TaskCore provides a simple way to distribute and manage tasks across multiple processes or machines using the filesystem as the backend.

## Features

- **Simple Setup**: No external dependencies - uses only Python standard library
- **Filesystem-based**: Tasks are stored as JSON files on disk
- **Multi-process Support**: Built-in support for distributed task processing
- **Fault Tolerant**: Automatic task recovery and stale task reclamation
- **Atomic Operations**: Safe concurrent access using atomic file operations
- **SLURM Integration**: Works seamlessly with SLURM job schedulers

## Installation

```bash
pip install taskcore
```

Or install from source:

```bash
git clone https://github.com/goncalorafaria/taskcore
cd taskcore
pip install -e .
```

## Quick Start

### Basic Usage

```python
from taskcore import FileSystemTaskQueueClient

# Initialize the task queue
queue = FileSystemTaskQueueClient("/path/to/task/directory")

# Add tasks to the queue
task_config = {
    "learning_rate": 1e-4,
    "batch_size": 32,
    "epochs": 100,
    "model_name": "bert-base-uncased"
}

task_id = queue.add_task(task_config)
print(f"Added task with ID: {task_id}")

# Process tasks
def init_function(**config):
    # Initialize your model/trainer here
    trainer = create_trainer(**config)
    return trainer, {"status": "initialized"}

def run_function(trainer):
    # Run your training/processing here
    trainer.train()

# Fetch and run tasks
while queue.fetch_and_run_task(init_func=init_function, func=run_function):
    pass
```

### SQLite Backend

TaskCore also provides an SQLite backend for users who want transactional task
claims, indexed status queries, and simpler stale-task recovery:

```python
from taskcore import SQLiteTaskQueueClient

queue = SQLiteTaskQueueClient("/path/to/task/directory")

task_id = queue.add_task({
    "learning_rate": 1e-4,
    "batch_size": 32,
})

while queue.fetch_and_run_task(init_func=init_function, func=run_function):
    pass
```

The filesystem backend remains available as `FileSystemTaskQueueClient` and keeps
the inspectable `pending/`, `running/`, and `finished/` JSON-file layout. The
SQLite backend stores tasks in `taskcore.sqlite3`; inspect it through the Python
API, the CLI, or SQL queries rather than by reading task JSON files directly.

### Multi-Process Example

```python
import os
from taskcore import FileSystemTaskQueueClient

def main():
    # Use LOCAL_RANK for multi-process setups (e.g., with torch.distributed)
    rank = int(os.environ.get("LOCAL_RANK", 0))
    queue = FileSystemTaskQueueClient("/path/to/tasks", rank=rank)
    
    def init_func(**config):
        # Initialize your distributed training setup
        trainer = create_distributed_trainer(**config)
        return trainer, {"wandb_link": trainer.wandb_link}
    
    def run_func(trainer):
        trainer.train()
    
    # Process tasks until none are available
    while queue.fetch_and_run_task(init_func=init_func, func=run_func):
        pass

if __name__ == "__main__":
    main()
```

### Task Generation Example

```python
from taskcore import FileSystemTaskQueueClient

# Initialize queue
queue = FileSystemTaskQueueClient("/path/to/task/directory")

# Define hyperparameter sweep
learning_rates = [1e-4, 1e-5, 1e-6]
batch_sizes = [16, 32, 64]
model_names = ["bert-base-uncased", "roberta-base"]

# Generate all combinations
for lr in learning_rates:
    for batch_size in batch_sizes:
        for model_name in model_names:
            config = {
                "learning_rate": lr,
                "batch_size": batch_size,
                "model_name": model_name,
                "epochs": 100,
                "wandb_project": "my-experiment"
            }
            
            task_id = queue.add_task(config)
            print(f"Added task {task_id}: {config}")
```

## API Reference

### FileSystemTaskQueueClient

The filesystem client class. Use this backend when you want the traditional
`pending/`, `running/`, and `finished/` directories with one JSON file per task.

### SQLiteTaskQueueClient

The SQLite client class. It supports the same high-level task lifecycle methods
as `FileSystemTaskQueueClient`, but stores task state in `taskcore.sqlite3`
instead of status directories.

#### Constructor

```python
FileSystemTaskQueueClient(base_dir: str, rank: int = 0)
SQLiteTaskQueueClient(base_dir: str, rank: int = 0)
```

- `base_dir`: Directory where task files or the SQLite database are stored
- `rank`: Process rank for multi-process setups (default: 0)

#### Methods

##### `add_task(task_dict: Dict) -> str`
Add a new task to the queue.

```python
task_id = queue.add_task({"param": "value"})
```

##### `fetch_and_run_task(init_func: Callable, func: Callable) -> bool`
Fetch a task, initialize it with `init_func`, and run it with `func`.

```python
def init_func(**config):
    return trainer, extra_info

def run_func(trainer):
    trainer.train()

success = queue.fetch_and_run_task(init_func, run_func)
```

##### `fetch_task() -> Tuple[str, Dict]`
Fetch the next available task.

```python
task_id, config = queue.fetch_task()
```

##### `finish_current_task()`
Mark the current task as completed.

##### `release_current_task()`
Release the current task back to the pending queue.

##### `edit_current_task(new_dict: Dict)`
Update the current task's configuration.

##### `get_current_task() -> Dict`
Get the configuration of the current task.

##### `has_task() -> bool`
Check if a task is currently being processed.

## Directory Structure

The filesystem backend creates the following directory structure:

```
base_dir/
├── pending/     # Tasks waiting to be processed
├── running/     # Tasks currently being processed
└── finished/    # Completed tasks
```

The SQLite backend stores its database at:

```
base_dir/
└── taskcore.sqlite3
```

## CLI

The CLI can show status for either backend:

```bash
python -m taskcore.cli /path/to/tasks status
python -m taskcore.cli /path/to/tasks show
```

By default, the CLI infers the backend. If `pending/`, `running/`, or
`finished/` exists, it uses the filesystem backend. Otherwise, if
`taskcore.sqlite3` exists, it uses SQLite. You can override inference explicitly:

```bash
python -m taskcore.cli /path/to/tasks show --backend=sqlite
python -m taskcore.cli /path/to/tasks show --backend=filesystem
```

## Environment Variables

TaskCore uses these environment variables for distributed processing:

- `SLURM_JOB_ID`: Job ID from SLURM scheduler
- `MY_JOB_ID`: Fallback job ID if SLURM_JOB_ID is not available
- `LOCAL_RANK`: Process rank in multi-process setups

## Error Handling

Tasks that fail during execution are automatically released back to the pending queue:

```python
try:
    queue.fetch_and_run_task(init_func, run_func)
except Exception as e:
    print(f"Task failed: {e}")
    # Task is automatically released back to pending queue
```

## Stale Task Recovery

TaskCore automatically reclaims stale tasks (default: 4 hours timeout):

```python
# Custom timeout in seconds
queue.queue.reclaim_stale_tasks(timeout_seconds=3600)  # 1 hour
```

## License

MIT License - see [LICENSE](LICENSE) file for details.
