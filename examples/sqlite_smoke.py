import glob
import os
import tempfile

from taskcore import SQLiteTaskQueueClient


def main():
    os.environ["MY_JOB_ID"] = "taskcore-sqlite-smoke"
    for path in glob.glob("/tmp/taskcore-sqlite-smoke_*.json"):
        os.remove(path)

    with tempfile.TemporaryDirectory() as base_dir:
        client = SQLiteTaskQueueClient(base_dir, timeout=1)
        first_id = client.add_task({"x": 1})
        second_id = client.add_task({"x": 2})

        assert client.num_pending_tasks() == 2

        task_id, config = client.fetch_task()
        assert task_id == first_id
        assert config == {"x": 1}

        client.edit_current_task({"x": 1, "task_id": task_id})
        assert client.get_current_task()["task_id"] == task_id
        client.finish_current_task()
        assert client.num_finished_tasks() == 1

        second_client = SQLiteTaskQueueClient(base_dir, timeout=1)
        task_id, config = second_client.fetch_task()
        assert task_id == second_id
        assert config == {"x": 2}

        second_client.release_current_task()
        assert second_client.num_pending_tasks() == 1

    for path in glob.glob("/tmp/taskcore-sqlite-smoke_*.json"):
        os.remove(path)

    print("sqlite smoke ok")


if __name__ == "__main__":
    main()
