import json
from pathlib import Path

from taskcore import FileSystemTaskQueueClient, SQLiteTaskQueueClient

try:
    from termcolor import colored
except ImportError:
    def colored(text, *args, **kwargs):
        return text


def infer_backend(base_dir: str) -> str:
    base_path = Path(base_dir)
    sqlite_path = base_path / "taskcore.sqlite3"
    filesystem_dirs = [
        base_path / "pending",
        base_path / "running",
        base_path / "finished",
    ]

    if any(path.exists() for path in filesystem_dirs):
        return "filesystem"
    if sqlite_path.exists():
        return "sqlite"
    return "filesystem"


def make_client(base_dir: str, backend: str = "auto"):
    if backend == "auto":
        backend = infer_backend(base_dir)

    if backend == "filesystem":
        return FileSystemTaskQueueClient(base_dir)
    if backend == "sqlite":
        return SQLiteTaskQueueClient(base_dir)

    raise ValueError(f"Invalid backend: {backend}")


def show_filesystem(client):
    print(colored(f"Pending tasks: {client.num_pending_tasks()}", "red", attrs=["bold"]))
    for task_file in client.queue.pending_dir.iterdir():
        task_dict = client.queue.get_task_dict(task_file)
        print(f"[{colored(task_file.name, 'green')}] {json.dumps(task_dict, indent=4)}")

    print(colored(f"Running tasks: {client.num_running_tasks()}", "green", attrs=["bold"]))
    for task_file in client.queue.running_dir.iterdir():
        task_dict = client.queue.get_task_dict(task_file)
        print(f"[{colored(task_file.name, 'green')}] {json.dumps(task_dict, indent=4)}")


def show_sqlite(client):
    for status, color in [("pending", "red"), ("running", "green")]:
        tasks = client.queue.list_tasks(status)
        print(colored(f"{status.title()} tasks: {len(tasks)}", color, attrs=["bold"]))
        for task in tasks:
            task_id = task.pop("id")
            payload = task.pop("payload")
            print(f"[{colored(task_id, 'green')}] {json.dumps(payload, indent=4)}")


def main(base_dir: str, mode: str = "status", backend: str = "auto"):
    backend = infer_backend(base_dir) if backend == "auto" else backend
    client = make_client(base_dir, backend)
    
    if mode == "status":
        print(colored(f"Pending tasks: {client.num_pending_tasks()}", 'red', attrs=['bold']))
        print(colored(f"Running tasks: {client.num_running_tasks()}", 'green', attrs=['bold']))
        print(colored(f"Finished tasks: {client.num_finished_tasks()}", 'blue', attrs=['bold']))
        
    elif mode == "show":
        if backend == "filesystem":
            show_filesystem(client)
        elif backend == "sqlite":
            show_sqlite(client)
            
    else:
        raise ValueError(f"Invalid mode: {mode}")
 


if __name__ == "__main__":
    import fire
    fire.Fire(main)