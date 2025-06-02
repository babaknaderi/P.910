import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import sem

# Define the folder containing the CSV files
source_folder = r"c:\Users\vigopal\source\repos\P.910\src\Teleport_b_05_19_2025_prolific"

# Function to read and merge CSV files
def merge_csv_files():
    # Find all CSV files ending with "_merged_votes_per_clip"
    csv_files = [f for f in os.listdir(source_folder) if "_merged_votes_per_clip" in f and f.endswith(".csv")]
    
    # Initialize an empty list to store DataFrames
    dataframes = []
    
    for file in csv_files:
        file_path = os.path.join(source_folder, file)
        print(f"Reading file: {file_path}")
        df = pd.read_csv(file_path)
        
        # Add "Question" column based on the last part of column names starting with "MOS_"
        df["Question"] = df.filter(like="MOS_").columns[0].split("_")[-1]
        
        dataframes.append(df)
    
    # Merge all DataFrames
    merged_df = pd.concat(dataframes, ignore_index=True)
    print("All files merged successfully.")
    return merged_df

# Function to expand vote columns into a common column "MOS"
def expand_votes_to_mos(df):
    # Identify columns starting with "vote_"
    vote_columns = [col for col in df.columns if col.startswith("vote_")]
    
    # Melt the DataFrame to create a common "MOS" column
    expanded_df = df.melt(
        id_vars=[col for col in df.columns if col not in vote_columns],  # Keep other columns
        value_vars=vote_columns,  # Columns to unpivot
        var_name="Vote_Type",  # New column for vote type
        value_name="MOS"  # New column for MOS values
    )
    print("Vote columns expanded into 'MOS'.")
    
    # Keep only the columns "model", "Question", and "MOS"
    expanded_df = expanded_df[["model_name", "Question", "MOS"]]
    
    # Rename "reepy" in the "Question" column to "Not Creepy" and adjust the "MOS" value
    expanded_df.loc[expanded_df["Question"].str.contains("reepy", na=False), "Question"] = "Not Creepy"
    expanded_df.loc[expanded_df["Question"] == "Not Creepy", "MOS"] = 6 - expanded_df["MOS"]
    
    # Combine specific model_name values into "Real"
    real_models = ["livelink", "meta_real", "mya_init_real_emotions", "mya_init_speak_emotions", "omnihuman1_real"]
    expanded_df["model"] = expanded_df["model_name"].replace(real_models, "Real")
    
    # Sort the DataFrame based on the average of the MOS column in descending order
    expanded_df = expanded_df.sort_values(by="MOS", ascending=False)
    
    return expanded_df

# Function to plot the average MOS per question per model with confidence interval
def plot_average_mos_with_ci(df):
    # Group by model and question, calculate mean and confidence interval
    summary = df.groupby(["model_name", "Question"]).agg(
        avg_mos=("MOS", "mean"),
        ci=("MOS", lambda x: sem(x) * 1.96)  # 95% confidence interval
    ).reset_index()
    
    # Sort values by average MOS in descending order
    summary = summary.sort_values(by="avg_mos", ascending=False)
    summary_merge = summary.groupby(['model_name']).agg(avg_mos=('avg_mos', 'mean')).reset_index()
    summary_merge = summary_merge.sort_values(by="avg_mos", ascending=False)
    
    # Plot using seaborn
    # plt.figure(figsize=(10, 6))
    sns.catplot(
        data=df,
        x="model_name",
        y="MOS",
        hue="Question",
        errorbar=('ci', 95),
        kind="bar",
        order=summary_merge["model_name"]
    )
    
    plt.title("Average MOS per Question per Model with Confidence Interval")
    plt.ylabel("Average MOS")
    plt.xlabel("Model")
    # change the y-axis to be 1-5
    plt.ylim(1, 5)
    # plt.legend(title="Question", bbox_to_anchor=(1.05, 1), loc='upper right')
    plt.grid(axis='y')
    plt.xticks(rotation=90)
    # plt.tight_layout()
    plt.show()

def get_gold_clip_category(row):
    diff_person = ["2d6c681f-dcf1-45c4-b867-88701028d36c",
                    "d6bdae02-e0b7-4377-8edf-3a48915aab00",
                    "ed4a90d8-fe3f-42e5-873a-7d91f410e25c",
                    "bc5f3e5d-d868-4e12-b9e5-b4c59b987f01",
                    "c2718ddd-04fb-42a7-942f-da7bd0fff9d9",
                    "9296c6b2-7e54-4f15-a7c6-e5bd46a0cd99",
                    "ac6d93c9-238c-405b-97e4-1d467b9a52a7",
                    "6e593f4e-c56b-4a5c-ba2d-b8b8a0cbf375"]
    diff_person = [str(i) for i in diff_person]

    same_person = ["69bdbc1d-3900-45f0-9b9c-0f21b0be7b0a",
                    "5e4ae658-cf1b-43c6-a98d-6534b881a9ab",
                    "8e195df6-5833-46d6-b445-e53ac661b914",
                    "fbdcdb50-e1bb-401a-afea-0e16ab18b48a",
                    "e369bb8e-bdbb-4ae2-9ba7-d0349d142b91",
                    "31cbf992-6a2d-4309-a5b5-32fcada20899",
                    "a83dd6aa-ec89-4a11-9080-efa91c42a760",
                    "84199e16-b1e1-49f8-ae62-8cedfbd9a1a5"]
    same_person = [str(i) for i in same_person]

    wrong_emo = ["440d385b-a175-446a-ac2f-1385fccc1799",
                    "733bde9f-7970-4ef3-9327-bba27b1118e2",
                    "7a5815ea-f7cd-46a8-9fd8-367e14571255",
                    "0b1de9bc-92bd-426a-8bd3-ba925ca90492",
                    "bfde3062-7aa8-4e47-861a-8ef2fd54334b",
                    "a35bedc9-06fc-4370-8a60-a8647354b1b4",
                    "c1155998-9141-42be-b675-fa59845b2981",
                    "37677efa-2ab9-4a7a-bc83-05d439a84841"]
    wrong_emo = [str(i) for i in wrong_emo]

    wrong_emo_diff_person  =  ["6cd2eda9-6575-468a-bc9a-75743f3e7697",
                                "142b1b4f-f4ff-43ec-9ec6-81167c1a0152",
                                "9928eba0-5697-4724-91ac-e5e95d8806cd",
                                "34d1d773-69da-4829-9cdb-024425c6cd9b"]
    wrong_emo_diff_person = [str(i) for i in wrong_emo_diff_person]

    wrong_gesture = ["09ad4c8d-237b-4db3-b6f5-6a5e1e8e8a1c",
                    "c33bb573-52a2-4d83-af58-1072a1852413",
                    "72b8948f-ce08-493e-afc1-fb58869ab039",
                    "bc95d135-2848-4ffd-ae60-55842a990456"]
    wrong_gesture = [str(i) for i in wrong_gesture]

    wrong_gesture_diff_person = ["6b02d7fc-a0fc-4afd-8a77-d31b8a16a04b",
                                    "46fc91ce-560b-4fee-a33a-2407cc2ba0c5",
                                    "19e99ff8-67cb-4b0f-98b4-fa0279cc30d1",
                                    "4ee9789b-5023-4b23-a9de-7db45ad0f774"]
    wrong_gesture_diff_person = [str(i) for i in wrong_gesture_diff_person]

    filebasename = os.path.basename(row['file_url']).split('.mp4')[0]
    if filebasename in diff_person:
        return "GOLD__Different Person"
    elif filebasename in same_person:
        return "Real"
    elif filebasename in wrong_emo:
        return "GOLD__Wrong Emotion"
    elif filebasename in wrong_emo_diff_person:
        return "GOLD__Wrong Emotion Different Person"
    elif filebasename in wrong_gesture:
        return "GOLD__Wrong Gesture"
    elif filebasename in wrong_gesture_diff_person:
        return "GOLD__Wrong Gesture Different Person"
    else:
        return "Unknown"

if __name__ == "__main__":
    # Step 1: Merge CSV files
    merged_df = merge_csv_files()    

    # Step 1.1: Rename the modle_name if it contains omnihuman1 to the file name
    merged_df['model_name'] = merged_df.apply(lambda x: x['model_name'].replace('OmniHuman1', os.path.basename(x['file_url']).split('.mp4')[0]) if 'OmniHuman1' in x['model_name'] else x['model_name'], axis=1)

    # Step 1.2: correct the model name for the file names
    merged_df['model_name'] = merged_df.apply(lambda x: x['model_name'].replace('side_by_side_rename', get_gold_clip_category(x)) if 'side_by_side_rename' in x['model_name'] else x['model_name'], axis=1)
    
    # Step 1.3: dump the file to a csv file
    merged_df.to_csv(os.path.join(source_folder, "merged_votes_per_clip.csv"), index=False)

    # drop all rows where model_name starts with "GOLD__"
    merged_df = merged_df[~merged_df['model_name'].str.startswith("GOLD__")]

    # Step 2: Expand vote columns into "MOS"
    final_df = expand_votes_to_mos(merged_df)

    # Step 3: Save the final DataFrame to a new CSV file
    output_file = os.path.join(source_folder, "merged_and_expanded_votes.csv")
    final_df.to_csv(output_file, index=False)
    print(f"Final merged and expanded file saved to: {output_file}")
    
    # Step 4: Plot the average MOS with confidence interval
    plot_average_mos_with_ci(final_df)