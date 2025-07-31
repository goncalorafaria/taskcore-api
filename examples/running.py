from taskcore import FileSystemTaskQueueClient
from bonvoyage.trainer import create_trainer
import os 


def main(base_dir="/gscratch/ark/graf/bonvoyage/test-sweep-base/"):
    client = FileSystemTaskQueueClient(base_dir,rank=int(os.environ.get("LOCAL_RANK", 0)))


    def init_func(**config):
        trainer=create_trainer(**config)
        return trainer, {"wandb_link": trainer.wandb_link}

    def func(trainer):
        trainer.train()
            
    while client.fetch_and_run_task(
            init_func=init_func, 
            func=func):
            
        pass
        

if __name__ == "__main__":
    # Check for wandb_links in pending tasks before starting the main loop
    main()