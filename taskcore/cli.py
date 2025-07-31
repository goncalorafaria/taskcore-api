from taskcore import FileSystemTaskQueueClient
import json
from termcolor import colored


def main(base_dir: str, mode:str="status"):
    client = FileSystemTaskQueueClient(base_dir)
    
    if mode == "status":
        print(colored(f"Pending tasks: {client.num_pending_tasks()}", 'red', attrs=['bold']))
        print(colored(f"Running tasks: {client.num_running_tasks()}", 'green', attrs=['bold']))
        print(colored(f"Finished tasks: {client.num_finished_tasks()}", 'blue', attrs=['bold']))
        
    elif mode == "show":
        
        print(colored(f"Pending tasks: {client.num_pending_tasks()}", 'red', attrs=['bold']))
        for task_file in client.queue.pending_dir.iterdir():
            #print("--"*40)
            task_dict = client.queue.get_task_dict(task_file)
            print(json.dumps(task_dict, indent=4))
            print(f"[{colored(task_file.name, 'green')}] {json.dumps(task_dict, indent=4)}")
            
        # colored(e.get('variant'),'green')
        print(colored(f"Running tasks: {client.num_running_tasks()}", 'green', attrs=['bold']))
        for task_file in client.queue.running_dir.iterdir():
            #print("--"*40)
            
            task_dict = client.queue.get_task_dict(task_file)
            print(f"[{colored(task_file.name, 'green')}]{json.dumps(task_dict, indent=4)}")
            #print(json.dumps(task_dict, indent=4))
            
    else:
        raise ValueError(f"Invalid mode: {mode}")
 


if __name__ == "__main__":
    import fire
    fire.Fire(main)