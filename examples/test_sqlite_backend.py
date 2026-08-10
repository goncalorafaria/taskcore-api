import glob
import os
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from taskcore import SQLiteTaskQueueClient
from taskcore.cli import infer_backend, make_client
from taskcore.sqlite import PENDING, RUNNING, SQLiteTaskQueue


class SQLiteTaskCoreTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.base_dir = self.tmpdir.name
        self.old_my_job_id = os.environ.get("MY_JOB_ID")
        self.old_slurm_job_id = os.environ.get("SLURM_JOB_ID")
        self.job_id = f"taskcore-test-{uuid.uuid4()}"
        os.environ.pop("SLURM_JOB_ID", None)
        os.environ["MY_JOB_ID"] = self.job_id
        self._cleanup_tid_files()

    def tearDown(self):
        self._cleanup_tid_files()
        if self.old_my_job_id is None:
            os.environ.pop("MY_JOB_ID", None)
        else:
            os.environ["MY_JOB_ID"] = self.old_my_job_id

        if self.old_slurm_job_id is None:
            os.environ.pop("SLURM_JOB_ID", None)
        else:
            os.environ["SLURM_JOB_ID"] = self.old_slurm_job_id

        self.tmpdir.cleanup()

    def _cleanup_tid_files(self):
        for path in glob.glob(f"/tmp/{self.job_id}_*.json"):
            os.remove(path)

    def test_add_fetch_edit_finish_and_release(self):
        client = SQLiteTaskQueueClient(self.base_dir, timeout=1)
        first_id = client.add_task({"x": 1})
        second_id = client.add_task({"x": 2})

        self.assertEqual(client.num_pending_tasks(), 2)
        self.assertEqual(client.num_running_tasks(), 0)
        self.assertEqual(client.num_finished_tasks(), 0)

        task_id, config = client.fetch_task()
        self.assertEqual(task_id, first_id)
        self.assertEqual(config, {"x": 1})
        self.assertTrue(client.has_task())
        self.assertEqual(client.num_pending_tasks(), 1)
        self.assertEqual(client.num_running_tasks(), 1)

        client.edit_current_task({"x": 1, "task_id": task_id})
        self.assertEqual(client.get_current_task()["task_id"], task_id)

        client.finish_current_task()
        self.assertFalse(client.has_task())
        self.assertEqual(client.num_finished_tasks(), 1)

        second_client = SQLiteTaskQueueClient(self.base_dir, timeout=1)
        task_id, config = second_client.fetch_task()
        self.assertEqual(task_id, second_id)
        self.assertEqual(config, {"x": 2})

        second_client.release_current_task()
        self.assertEqual(second_client.num_pending_tasks(), 1)
        self.assertEqual(second_client.num_running_tasks(), 0)

    def test_two_clients_claim_different_tasks(self):
        producer = SQLiteTaskQueueClient(self.base_dir)
        first_id = producer.add_task({"name": "first"})
        second_id = producer.add_task({"name": "second"})

        first_client = SQLiteTaskQueueClient(self.base_dir)
        second_client = SQLiteTaskQueueClient(self.base_dir)

        claimed_first, _ = first_client.fetch_task()
        claimed_second, _ = second_client.fetch_task()

        self.assertEqual({claimed_first, claimed_second}, {first_id, second_id})
        self.assertNotEqual(claimed_first, claimed_second)
        self.assertEqual(producer.num_pending_tasks(), 0)
        self.assertEqual(producer.num_running_tasks(), 2)

    def test_stale_running_tasks_are_reclaimed(self):
        queue = SQLiteTaskQueue(self.base_dir)
        task_id = queue.add_task({"stale": True})
        self.assertEqual(queue.fetch_task(), task_id)
        self.assertEqual(queue.num_running_tasks(), 1)

        old_timestamp = time.time() - 3600
        with queue._connect() as conn:
            conn.execute(
                "UPDATE tasks SET updated_at = ? WHERE id = ?",
                (old_timestamp, task_id),
            )

        queue.reclaim_stale_tasks(timeout_seconds=1)
        tasks = queue.list_tasks()
        self.assertEqual(tasks[0]["status"], PENDING)
        self.assertEqual(queue.num_pending_tasks(), 1)
        self.assertEqual(queue.num_running_tasks(), 0)

    def test_fetch_and_run_success_updates_and_finishes_task(self):
        client = SQLiteTaskQueueClient(self.base_dir)
        task_id = client.add_task({"value": 3})

        def init_func(**config):
            return config["value"], {"extra": "metadata"}

        seen = []

        def run_func(trainer):
            seen.append(trainer)

        self.assertTrue(client.fetch_and_run_task(init_func, run_func))
        self.assertEqual(seen, [3])
        self.assertEqual(client.num_finished_tasks(), 1)

        finished = client.queue.list_tasks("finished")
        self.assertEqual(finished[0]["id"], task_id)
        self.assertEqual(finished[0]["payload"]["extra"], "metadata")
        self.assertEqual(finished[0]["payload"]["task_id"], task_id)

    def test_fetch_and_run_failure_releases_task(self):
        client = SQLiteTaskQueueClient(self.base_dir)
        task_id = client.add_task({"value": 3})

        def init_func(**config):
            return config["value"], {}

        def run_func(trainer):
            raise RuntimeError(f"failed with {trainer}")

        self.assertFalse(client.fetch_and_run_task(init_func, run_func))
        self.assertEqual(client.num_pending_tasks(), 1)
        self.assertEqual(client.num_running_tasks(), 0)

        task = client.queue.list_tasks(PENDING)[0]
        self.assertEqual(task["id"], task_id)
        self.assertIn("failed with 3", task["error"])

    def test_cli_backend_inference_and_factory(self):
        self.assertEqual(infer_backend(self.base_dir), "filesystem")

        sqlite_client = SQLiteTaskQueueClient(self.base_dir)
        sqlite_client.add_task({"via": "sqlite"})
        self.assertEqual(infer_backend(self.base_dir), "sqlite")
        self.assertIsInstance(make_client(self.base_dir), SQLiteTaskQueueClient)

        Path(self.base_dir, "pending").mkdir()
        self.assertEqual(infer_backend(self.base_dir), "filesystem")

    def test_list_tasks_decodes_payload_and_metadata(self):
        queue = SQLiteTaskQueue(self.base_dir)
        task_id = queue.add_task({"a": 1})
        queue.fetch_task()

        tasks = queue.list_tasks(RUNNING)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["id"], task_id)
        self.assertEqual(tasks[0]["payload"], {"a": 1})
        self.assertEqual(tasks[0]["attempts"], 1)
        self.assertIsNotNone(tasks[0]["started_at"])


if __name__ == "__main__":
    unittest.main()
