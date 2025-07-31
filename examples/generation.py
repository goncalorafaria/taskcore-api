from taskcore import FileSystemTaskQueueClient

base_dir = "/gscratch/ark/graf/bonvoyage/test-sweep-base/"
queue = FileSystemTaskQueueClient(base_dir)


lr_options = [ 4e-6 ]
beta_options = [1.0]
ppo_epochs_options = [1]
method_options = ["sbon32_norm"]
onpolicy_expert_options = [False]
wd_options = [1e-2]


## cross product
for lr in lr_options:
    for beta in beta_options:
        for ppo_epochs in ppo_epochs_options:
            for method in method_options:
                for onpolicy_expert in onpolicy_expert_options:
                    for wd in wd_options:
                        config = {
                            "method": method,
                            "num_epochs": 80,
                            "learning_rate": lr,
                            "weight_decay": wd,
                            "beta": beta,
                            "wandb_project_name": "bon-norm-fixed-mathtest",
                            "ppo_epochs": ppo_epochs,
                            "onpolicy_expert": onpolicy_expert,
                            "reward_model_path":"Skywork/Skywork-Reward-V2-Llama-3.2-1B",#"allenai/Llama-3.1-Tulu-3-8B-SFT",
                            "train_dataset":"tulu_persona_math",
                            "test_dataset":"tulu_math_test",
                        }
                        
                        tid = queue.add_task(config)
                        print(f"Added task {config} - {tid}")






