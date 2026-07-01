import json
import os
import sqlite3
import time
import traceback
import uuid
from pathlib import Path
from typing import Callable, Dict, Optional

from taskcore.base import TaskQueueClient, dummy_run_func
from taskcore.dist import atomic_read, atomic_write, get_unique_job_id, wait_for_tid_file


PENDING = "pending"
RUNNING = "running"
FINISHED = "finished"


class SQLiteTaskQueue:
    def __init__(self, base_dir: str, db_name: str = "taskcore.sqlite3"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.base_dir / db_name
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    owner TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    error TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_status_created "
                "ON tasks(status, created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_status_updated "
                "ON tasks(status, updated_at)"
            )

    def add_task(self, task_dict: Dict):
        task_id = str(uuid.uuid4())
        now = time.time()
        payload = json.dumps(task_dict)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO tasks (id, status, payload, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (task_id, PENDING, payload, now, now),
            )
        return task_id

    def _num_tasks_with_status(self, status: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM tasks WHERE status = ?",
                (status,),
            ).fetchone()
        return int(row["count"])

    def num_pending_tasks(self) -> int:
        return self._num_tasks_with_status(PENDING)

    def num_running_tasks(self) -> int:
        return self._num_tasks_with_status(RUNNING)

    def num_finished_tasks(self) -> int:
        return self._num_tasks_with_status(FINISHED)

    def read_task(self, task_id: str) -> Dict:
        task_id = self._normalize_task_id(task_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
        if row is None:
            raise FileNotFoundError(f"No task found with id {task_id}")
        return json.loads(row["payload"])

    def fetch_task(self) -> Optional[str]:
        now = time.time()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT id FROM tasks
                WHERE status = ?
                ORDER BY created_at
                LIMIT 1
                """,
                (PENDING,),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None

            task_id = row["id"]
            conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    owner = ?,
                    started_at = ?,
                    updated_at = ?,
                    finished_at = NULL,
                    attempts = attempts + 1,
                    error = NULL
                WHERE id = ? AND status = ?
                """,
                (RUNNING, get_unique_job_id(), now, now, task_id, PENDING),
            )
            conn.execute("COMMIT")
            return task_id
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def finish_task(self, task_id: str):
        task_id = self._normalize_task_id(task_id)
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    updated_at = ?,
                    finished_at = ?,
                    owner = NULL
                WHERE id = ?
                """,
                (FINISHED, now, now, task_id),
            )

    def release_task(self, task_id: str, error: Optional[str] = None):
        task_id = self._normalize_task_id(task_id)
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    updated_at = ?,
                    started_at = NULL,
                    finished_at = NULL,
                    owner = NULL,
                    error = ?
                WHERE id = ?
                """,
                (PENDING, now, error, task_id),
            )

    def reset_task_timer(self, task_id: str):
        task_id = self._normalize_task_id(task_id)
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET updated_at = ? WHERE id = ?",
                (now, task_id),
            )

    def reclaim_stale_tasks(self, timeout_seconds: int = 600):
        cutoff = time.time() - timeout_seconds
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    updated_at = ?,
                    started_at = NULL,
                    owner = NULL,
                    error = 'reclaimed stale task'
                WHERE status = ? AND updated_at < ?
                """,
                (PENDING, now, RUNNING, cutoff),
            )

    def get_task_dict(self, task_id: str) -> Dict:
        return self.read_task(task_id)

    def update_task_info(
        self,
        task_id: str,
        new_dict: Optional[Dict] = None,
    ):
        if new_dict is None:
            return

        task_id = self._normalize_task_id(task_id)
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET payload = ?, updated_at = ? WHERE id = ?",
                (json.dumps(new_dict), now, task_id),
            )

    def list_tasks(self, status: Optional[str] = None):
        query = (
            "SELECT id, status, payload, owner, created_at, updated_at, "
            "started_at, finished_at, attempts, error FROM tasks"
        )
        params = ()
        if status is not None:
            query += " WHERE status = ?"
            params = (status,)
        query += " ORDER BY created_at"

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()

        tasks = []
        for row in rows:
            task = dict(row)
            task["payload"] = json.loads(task["payload"])
            tasks.append(task)
        return tasks

    @staticmethod
    def _normalize_task_id(task_id: str) -> str:
        path = Path(str(task_id))
        if path.suffix == ".json":
            return path.stem
        return path.name


class SQLiteTaskQueueClient(TaskQueueClient):
    def __init__(self, base_dir: str, rank: int = 0, timeout: int = 60 * 60 * 4):
        self.queue = SQLiteTaskQueue(base_dir)
        super().__init__(self.queue, timeout)
        self.rank = rank

    def add_task(self, task_dict: Dict):
        return self.queue.add_task(task_dict)

    def fetch_task(self):
        job_id = get_unique_job_id()

        if self.rank == 0:
            result = super().fetch_task()
            if not result:
                print("No task found")
                raise RuntimeError("No task found")

            task_id, config = result
            shared_spot_name = _get_tid_file_path(job_id, task_id)
            atomic_write(task_id, shared_spot_name)
            return task_id, config

        time.sleep(15)
        tid_file = wait_for_tid_file(job_id)
        task_id = atomic_read(tid_file)
        config = self.read_task(task_id)
        return task_id, config

    def release_current_task(self):
        if self.current_task_file is None:
            raise RuntimeError("No task is currently fetched.")
        self.queue.release_task(self.current_task_file)
        self.current_task_file = None

    def fetch_and_run_task(self, init_func: Callable, func: Callable = dummy_run_func):
        task_id, config = self.fetch_task()
        print(f"Running task {config}")

        try:
            trainer, extra_info = init_func(**config)

            if self.rank == 0:
                config.update(extra_info)
                config["task_id"] = task_id
                self.edit_current_task(config)

            func(trainer)

            if self.rank == 0:
                self.finish_current_task()

            return True
        except Exception as e:
            print(f"Error running task {config}: {e}")
            traceback.print_exc()
            if self.rank == 0:
                self.queue.release_task(task_id, error=str(e))
                self.current_task_file = None
            return False
        finally:
            shared_spot_name = _get_tid_file_path(get_unique_job_id(), task_id)
            if self.rank == 0 and os.path.exists(shared_spot_name):
                os.remove(shared_spot_name)


def _get_tid_file_path(job_id: str, task_id: str) -> str:
    return f"/tmp/{job_id}_{task_id}.json"
