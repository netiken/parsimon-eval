import os
import json
from collections import defaultdict

cc_dict={
    "dctcp":"dctcp",
    "timely": "timely_vwin",
    "dcqcn": "dcqcn_paper_vwin",
    "hp": "hp",
}
def find_large_files(json_file):
    large_files = []
    with open(json_file, 'r') as f:
        data = json.load(f)
        
    file_list=[]
    for item_idx, item in enumerate(data):
        cc=item['cc']
        param_cc=round(item['param_cc'],1)
        file_name=f"/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/{item_idx}/ns3-config/fct_topology_flows_{cc_dict[cc]}_k{item['window']}_b1.0_p{param_cc}.txt"
        file_list.append(file_name)
    cc_cnt_dict=defaultdict(lambda:0)
    cc_cnt_dict_process=defaultdict(lambda:0)
    file_to_process=[]
    file_to_finished=[]
    file_to_wait=[]
    for item_idx, file_path in enumerate(file_list):
        # print(file_path)
        try:
            cc=data[item_idx]['cc']

            if os.path.exists(f"/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/{item_idx}/ns3-config/elapsed.txt"):
                cc_cnt_dict[cc]+=1
                file_to_finished.append(data[item_idx])
            elif os.path.exists(f"/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/{item_idx}/ns3-config/flows_path_map.txt"):
                cc_cnt_dict_process[cc]+=1
                file_to_process.append(data[item_idx])
            else:
                file_size = os.path.getsize(file_path)
            
                # Convert bytes to megabytes
                file_size_in_mb = file_size / (1024 * 1024)
            
                large_files.append((item_idx, file_size_in_mb))
                file_to_wait.append(data[item_idx])
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
    print(cc_cnt_dict)
    print(cc_cnt_dict_process)
    assert len(file_to_finished)+len(file_to_process)+len(file_to_wait)==len(file_list)
    print(f"files: {len(file_to_finished)},{len(file_to_process)},{len(file_to_wait)}")
    with open(f"mlsys_config.mix.json", 'w') as f:
        json.dump(file_to_finished, f, indent=2)
    with open(f"ns3_config.mix.json", 'w') as f:
        json.dump(file_to_process, f, indent=2)
    return large_files

if __name__ == "__main__":

    json_file = 'all_config.mix.json'  # Replace with your JSON file path
    
    # Find large files
    large_files = find_large_files(json_file)
    print(f"{len(large_files)} large files found")
    for item_idx, file_size_in_mb in large_files:
        print(f"{item_idx} ({file_size_in_mb} MB)")