import pandas as pd
import os


study_directory = r"C:\Users\babaknaderi\OneDrive - Microsoft\other_locations\data\studies\teleport\test_prolific\Teleport_pt_06_07_2025_prolific\Teleport_pt_test"

def get_condition_files(study_directory):
    """
    Get all condition files in the study directory.
    """
    condition_files = []
    for root, dirs, files in os.walk(study_directory):
        for file in files:
            if file.endswith('.csv') and '_votes_per_cond' in file.lower():
                condition_files.append(os.path.join(root, file))
    return condition_files


def merge_condition_files(condition_files):
    """
    Merge all condition files into a single DataFrame.
    """
    merged_df = None
    for file in condition_files:
        df = pd.read_csv(file)
        # check if there is a col with SUM_*, calculate sum* / n
        sum_cols = [col for col in df.columns if col.startswith('SUM_')]
        mos_cols = [col for col in df.columns if col.startswith('MOS_')]
        agg_col = None
        if sum_cols:
            for col in sum_cols:
                agg_col = col.replace('SUM_', 'PER_')
                df[agg_col] = df[col] / df['n']

        if mos_cols:
            for col in mos_cols:
                agg_col = col
        print(f"Processing file: {file}, agg_col: {agg_col}")
        if agg_col is None:
            print(f"No aggregation column found in file: {file}")
            continue
        # only keep columns: condition_name, n,  and agg_col        
        if merged_df is None:
            df =  df[['condition_name', 'n', agg_col]]
            merged_df = df.copy()
        else:
            df =  df[['condition_name',  agg_col]]
            merged_df = pd.merge(merged_df, df, on=['condition_name'], how='outer')    

    return merged_df

def normalize_model_name(model_name):
    """
    Normalize the model name by renaming the internal model names to MS1, MS2, etc.
    p3d* is renamed to MS1, avena* is renamed to MS2, holo3d* is renamed to MS3, avanti* is renamed to MS4 and mesh_realistic_0 is renamed to MS5.
    copresence* is renamed to Extenal1, copresence_pro* is renamed to Extenal2 and copresence_cg* is renamed to Extenal3.
    
    Args:
        model_name (str): The original model name.
        
    Returns:
        str: The normalized model name.
    """
    model_names = ['Real', 'OmniHuman1', 'LivePortrait', 'FantasyTalking', 'FADA_balanced', 'Meta', 'HRAvatar', 'MakeYourAnchor', 'EchoMimic', 'EMO2', 'Vlogger', 'CyberHost', 'SADTalker', 'MS1', 'TaoAvatar_body', 'TaoAvatar_upper', 'MS2', 'Unreal_Metahuman', 'External1', 'MeGA', 'External2', 'Audio2Photoreal_0', 'FATE', 'LUCAS', 'MS3', 'CAP4D', 'MS4', 'MS5']
    model_name_lower = model_name.lower()
    model_names_lower = {name.lower(): name for name in model_names}
    model_names_lower['livelink'] = 'Real'
    model_names_lower['omnihuman1_hands'] = 'OmniHuman1'
    model_names_lower['unreal_0'] = 'Unreal_Metahuman'


    if model_name.startswith("p3d"):
        return "MS1", "MS1" in model_names 
    elif model_name.startswith("avena"):
        return "MS2", "MS2" in model_names
    elif model_name.startswith("holo3d_0"):
        return "MS3", "MS3" in model_names
    elif model_name.startswith("avanti"):
        return "MS4", "MS4" in model_names
    elif model_name.startswith("mesh_realistic"):
        return "MS5", "MS5" in model_names
    elif model_name.startswith("copresence_"):
        if "pro" in model_name:
            return "External2", "External2" in model_names
        elif "cg" in model_name:
            return "External3", "External3" in model_names
        else:
            return "External1", "External1" in model_names
    elif model_name.lower() in model_names_lower.keys():
        return model_names_lower[model_name.lower()], True
    else:
        # return th emodelname as in the model_names list
        return model_name, False
    
list_of_files = get_condition_files(study_directory)
merged_df = merge_condition_files(list_of_files)
output_file = os.path.join(study_directory, 'merged_conditions.csv')

# update conditoon name normalize_model_name
merged_df[['condition_name_normalized', 'is_in_list']] = merged_df['condition_name'].apply(lambda x: pd.Series(normalize_model_name(x)))
merged_df.to_csv(output_file, index=False)