import json
import uuid
from pathlib import Path
import os
import time
import os
import time

import os
import json
import uuid
import time
from pathlib import Path
from typing import Optional, Dict
from taskcore.dist import atomic_write, get_unique_job_id,  wait_for_tid_file, get_tid_file_path
import traceback
from typing import Callable

class FileSystemTaskQueue:
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.pending_dir = self.base_dir / "pending"
        self.running_dir = self.base_dir / "running"
        self.finished_dir = self.base_dir / "finished"
        # Ensure directories exist
        for d in [self.pending_dir, self.running_dir, self.finished_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def add_task(self, task_dict: Dict):
        task_id = str(uuid.uuid4())
        task_file = self.pending_dir / f"{task_id}.json"
        with open(task_file, "w") as f:
            json.dump(task_dict, f)
        return task_id

    def read_task(self, task_id: str) -> Dict:
        task_file = self.running_dir / f"{task_id}.json"
        with open(task_file, "r") as f:
            return json.load(f)
    
    def fetch_task(self) -> Optional[str]:
        for task_file in self.pending_dir.iterdir():
            running_file = self.running_dir / task_file.name
            try:
                os.rename(task_file, running_file)  # atomic move
                return str(running_file)
            except FileNotFoundError:
                continue  # another worker got it
        return None

    def finish_task(self, running_file: str):
        running_path = Path(running_file)
        finished_file = self.finished_dir / running_path.name
        os.rename(running_path, finished_file)

    def reset_task_timer(self, running_file: str):
        now = time.time()
        os.utime(running_file, (now, now))

    def reclaim_stale_tasks(self, timeout_seconds: int = 600):
        now = time.time()
        for task_file in self.running_dir.iterdir():
            if now - task_file.stat().st_mtime > timeout_seconds:
                pending_file = self.pending_dir / task_file.name
                try:
                    os.rename(task_file, pending_file)
                except FileNotFoundError:
                    continue  # Already reclaimed

    def get_task_dict(self, task_file: str) -> Dict:
        with open(task_file, "r") as f:
            return json.load(f)

    def update_task_info(
        self,
        task_file: str,
        new_dict: Optional[Dict] = None,
    ):
        """
        Overwrite the entire task file with new_dict if provided.
        Does nothing if new_dict is None.
        """
        if new_dict is not None:
            path = Path(task_file)
            with open(path, "w") as f:
                json.dump(new_dict, f)
        
    
class TaskQueueClient:
    def __init__(self, queue: FileSystemTaskQueue):
        self.queue = queue
        self.current_task_file = None

        
    def read_task(self, task_file: str):
 
        return self.queue.read_task(task_file)
    
    def fetch_task(self):
        if self.current_task_file is not None:
            raise RuntimeError("A task is already fetched. Finish or release it before fetching another.")
        self.queue.reclaim_stale_tasks(60*60*4)  # Reclaim stale tasks before fetching
        task_file = self.queue.fetch_task()
        if task_file:
            self.current_task_file = task_file
            return self.current_task_file, self.get_current_task()
        return None

    def edit_current_task(self, new_dict):
        if self.current_task_file is None:
            raise RuntimeError("No task is currently fetched.")
        self.queue.update_task_info(self.current_task_file, new_dict)

    def finish_current_task(self):
        if self.current_task_file is None:
            raise RuntimeError("No task is currently fetched.")
        self.queue.finish_task(self.current_task_file)
        self.current_task_file = None

    def release_current_task(self):
        if self.current_task_file is None:
            raise RuntimeError("No task is currently fetched.")
        # Move the task back to pending
        task_path = Path(self.current_task_file)
        pending_file = self.queue.pending_dir / task_path.name
        os.rename(task_path, pending_file)
        self.current_task_file = None

    def get_current_task(self):
        if self.current_task_file is None:
            return None
        return self.queue.get_task_dict(self.current_task_file)

    def has_task(self):
        return self.current_task_file is not None
        

class FileSystemTaskQueueClient(TaskQueueClient):
    def __init__(self,base_dir: str, rank: int = 0):
        self.queue = FileSystemTaskQueue(base_dir)
        super().__init__(self.queue)
        self.rank = rank

    def fetch_task(self):
        job_id = get_unique_job_id()
        
        if self.rank == 0:
            # Main process fetches the next task
            result =super().fetch_task() ## fetch the task on main process
            if not result:
                print("No task found")
                raise RuntimeError("No task found")
            
            print(f"Fetched task {result}")
            tid, config = result

            shared_spot_name = get_tid_file_path(tid) ## get path for the shared spot
            atomic_write(tid, shared_spot_name)## write to a shared spot to tell other processes to fetch the task

            return tid, config
        else:
            time.sleep(15)
            tid = wait_for_tid_file(job_id) ## wait for new file in shared spot.
            
            tid = tid.split("_")[-1].split(".")[0] ## get the tid from the file name

            config = self.read_task(tid) ## broadcast the task to all processes
            
            return tid, config
        
    def fetch_and_run_task(self, init_func: Callable, func: Callable):
        
        tid, config = self.fetch_task() ## takes care of distributing a single task to all processes
     
        print(f"Running task {config}")

        try:
            trainer, extra_info = init_func(**config)
            
            if self.rank == 0:
                config.update(extra_info)
                #config["wandb_link"] = trainer.wandb_link
                config["task_id"] = tid
                self.edit_current_task(config)
            
            func(
                trainer,
            )
            
            if self.rank==0:
                self.finish_current_task()
                
            return True
                
        except Exception as e:
            print(f"Error running task {config}: {e}")
            #print("traceback:", e.__traceback__)
            traceback.print_exc()
            if self.rank==0:
                self.release_current_task()
                
            return False
        finally:
            
            shared_spot_name = get_tid_file_path(tid)
            # Clean up the tid file after use
            if self.rank==0 and shared_spot_name and os.path.exists(shared_spot_name):
                os.remove(shared_spot_name)
                
