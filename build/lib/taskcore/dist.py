

import os
import glob
import time

def atomic_write(data, filename):
    tmp_filename = filename + ".tmp"
    with open(tmp_filename, "w") as f:
        f.write(data)
    os.rename(tmp_filename, filename)  # Atomic on POSIX

def atomic_read(filename):
    with open(filename, "r") as f:
        return f.read().strip()
    

def wait_for_tid_file(job_id, timeout=60):
    pattern = f"/tmp/{job_id}_*.json"
    start = time.time()
    tid_file = None
    while True:
        files = [f for f in os.listdir("/tmp") if f.startswith(f"{job_id}_") and f.endswith(".json")]
        if files:
            # Get the youngest file (most recently modified)
            file_paths = [os.path.join("/tmp", f) for f in files]
            tid_file = max(file_paths, key=os.path.getmtime)
            # Wait for file to be non-empty
            if os.path.getsize(tid_file) > 0:
                return tid_file
        if time.time() - start > timeout:
            raise TimeoutError("Timeout waiting for tid file")
        time.sleep(0.1)
        
def get_tid_file_path(tid):
    tid_filename = os.path.basename(tid)
    unique_id = os.environ.get("SLURM_JOB_ID") or os.environ.get("MY_JOB_ID")
    if unique_id is None:
        raise RuntimeError("No unique job ID found in environment!")
    return f"/tmp/{unique_id}_{tid_filename}"

import uuid
import time

def get_unique_job_id():
    # Use SLURM_JOB_ID if available, else MY_JOB_ID, else generate and set MY_JOB_ID
    job_id = os.environ.get("SLURM_JOB_ID")
    if not job_id:
        job_id = os.environ.get("MY_JOB_ID")
        if not job_id:
            job_id = str(uuid.uuid4())
            os.environ["MY_JOB_ID"] = job_id
    return job_id