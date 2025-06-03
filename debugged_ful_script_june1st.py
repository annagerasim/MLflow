#!/home/ero6410/.conda/envs/UNITO_demo/bin/python
import os
import pandas as pd
import flowkit as fk
import json
import numpy as np
from memory_profiler import profile

#Processing the first half of the workspace

# --- setup workspace & get gate list ---
wsp = fk.Workspace(
    "/home/ero6410/flow_code/code_folder/Suchitra_learning_gating/21-May-2025.wsp",
    fcs_samples="/home/ero6410/flow_code/code_folder/Suchitra_learning_gating"
)
sample_ids   = wsp.get_sample_ids()
half_ids = sample_ids[0:(len(sample_ids)//2)]
for sid in half_ids:
    wsp.analyze_samples(sample_id=sid, verbose=False, use_mp=False)
results = wsp.get_analysis_report()
gate_names = results["gate_name"].unique().tolist()

del results
@profile
def collect_gated_events_by_gate(wsp):
    all_gated = dict()
    for sample_id in half_ids:
        for gate in gate_names:
            try:
                df = wsp.get_gate_events(sample_id, gate_name=gate)
                df['sample_id'] = sample_id 
                df[f"{gate}"] = 1
                if gate not in all_gated:
                    all_gated[gate] = [df]
                else:
                    all_gated[gate].append(df)
            except Exception as e:
                print(f"Failed to get gate '{gate}' for sample '{sample_id}': {e}")
                continue
    return all_gated

all_gated = collect_gated_events_by_gate(wsp)

for gate in all_gated:
    df = pd.concat(all_gated[gate], ignore_index=True)
    for col in gate_names:
        if col not in df.columns:
            df[col] = np.nan
    all_gated[gate] = df 
    
    
first_gate = next(iter(all_gated))      
sample_df = all_gated[first_gate]       
cols_for_rem_dupl = [c for c in sample_df.columns if c not in gate_names]

# -------second function--------          
@profile
def process_gating_hierarchy(gating_json, all_gated, cols_for_rem_dupl, gate_names):
    """
    Simplified gating hierarchy processor that treats all gate types the same.
    
    Args:
        gating_json (dict): Gating hierarchy JSON structure
        all_gated (dict): Dictionary of {gate_name: dataframe}
        cols_for_rem_dupl (list): Columns for duplicate removal
        gate_names (list): Marker columns to keep
        
    Returns:
        dict: Processed dataframes for each gate
    """
    # 1. Clean all DataFrames (replace NaN with 0)
    cleaned_gates = {gate: df.fillna(0) for gate, df in all_gated.items()}
    
    # Recursive processing function
    def process_node(node):
        node_name = node['name']
        node_df = cleaned_gates.get(node_name, pd.DataFrame())
        
        # Process children if they exist
        if 'children' in node and node['children']:
            # Combine all children (recursively processed)
            children_combined = pd.concat(
                [process_node(child) for child in node['children']],
                ignore_index=True
            )
            
            # If current node has its own dataframe, combine with children
            if not node_df.empty:
                combined = pd.concat([node_df, children_combined], ignore_index=True)
                return combined.groupby(cols_for_rem_dupl, as_index=False)[gate_names].max()
            return children_combined
        return node_df
    
    # Collect results for all nodes
    result = {}
    
    def collect_results(node):
        node_name = node['name']
        result[node_name] = process_node(node)
        if 'children' in node:
            for child in node['children']:
                collect_results(child)
    
    # Start processing from root's children (skip the root itself)
    for child in gating_json['children']:
        collect_results(child)
    
    return result

ws_json = wsp.get_gate_hierarchy('20230430_LT192 RML-04_001.fcs',output='json', indent=2)
ws_json_dict = json.loads(ws_json)
test_dfs = process_gating_hierarchy(ws_json_dict, all_gated, cols_for_rem_dupl, gate_names)
all_set = test_dfs["Singlets"]
path = "./sample_csvs_test_June3"
os.makedirs(path, exist_ok=True)  

for sample, df in all_set.groupby("sample_id"):
    out_path = os.path.join(path, f"{sample}.csv")
    df.to_csv(out_path, index=False)

    
all_set.to_csv("./first_half_concatenated_June3.csv", index=False)
