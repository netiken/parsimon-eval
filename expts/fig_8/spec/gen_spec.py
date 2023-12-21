import json
import os

def filter_json(json_file, output_file):
    # Read the JSON file
    with open(json_file, 'r') as f:
        data = json.load(f)

    # Filter based on the existence of corresponding files
    filtered_data = [item for item in data if not os.path.exists(f"/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/{item['id']}/ns3/records.csv")]
    print(len(filtered_data))
    # Export the filtered data to a new JSON file
    # with open(output_file, 'w') as f:
    #     json.dump(filtered_data, f, indent=2)

# Example usage
json_file_path = 'all.mix.json'  # Replace with your JSON file path
output_file_path = 'filtered.mix.json'  # Replace with the desired output file path

filter_json(json_file_path, output_file_path)
