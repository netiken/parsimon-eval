import os
import json
from collections import defaultdict

cc_dict={
    "dctcp":"dctcp",
    "timely": "timely_vwin",
    "dcqcn": "dcqcn_paper_vwin",
    "hp": "hp",
}
def find_large_files(json_file,shard_seed,dir_str=''):
    large_files = []
    with open(json_file, 'r') as f:
        data = json.load(f)
        
    file_list=[]
    for item_idx, item in enumerate(data):
        cc=item['cc']
        config_id=item['id']
        file_name=f"../data{dir_str}/{config_id}/ns3-config/{shard_seed}/fct_topology_flows_{cc_dict[cc]}.txt"
        file_list.append(file_name)
    cc_cnt_dict=defaultdict(lambda:0)
    file_to_finished=[]
    file_to_restart=[]
    file_to_wait=[]
    for item_idx, file_path in enumerate(file_list):
        # print(file_path)
        try:
            cc=data[item_idx]['cc']
            config_id=data[item_idx]['id']
            if not os.path.exists(file_path):
                cc_cnt_dict[cc]+=1
                file_to_finished.append(data[item_idx]['id'])
            else:
                file_size = os.path.getsize(file_path)
            
                # Convert bytes to megabytes
                file_size_in_mb = file_size / (1024 * 1024)
                if not os.path.exists(f"../data{dir_str}/{config_id}/ns3-config/{shard_seed}/flows.txt"):
                    file_to_restart.append(data[item_idx])
                else:
                    large_files.append((config_id, file_size_in_mb))
                    file_to_wait.append(data[item_idx])
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
    print(cc_cnt_dict)
    print(file_to_finished)
    assert len(file_to_finished)+len(file_to_restart)+len(file_to_wait)==len(file_list)
    print(f"files: {len(file_to_finished)},{len(file_to_restart)},{len(file_to_wait)}")
    with open(f"restart_config.mix.json", 'w') as f:
        json.dump(file_to_restart, f, indent=2)
    return large_files

if __name__ == "__main__":

    # shard_seed=1
    # dir_str=""
    # json_file=f'all_counterfactual.mix.json' 
    
    # shard_seed=2
    # dir_str=""
    # json_file = f'all_dctcp.mix.json' 
    
    shard_seed=4
    dir_str="_dctcp"
    json_file=f'all_counterfactual{dir_str}.mix_4.json' 
    
    # shard_seed=4
    # dir_str="_hpcc"
    # json_file=f'all_counterfactual{dir_str}.mix.json' 
    
    # Find large files
    large_files = find_large_files(json_file,shard_seed=shard_seed,dir_str=dir_str)
    print(f"{len(large_files)} large files found")
    for item_idx, file_size_in_mb in large_files:
        print(f"{item_idx} ({file_size_in_mb} MB)")
