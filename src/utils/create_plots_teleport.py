import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import sem

# Define the folder containing the CSV files
source_folder = r"c:\Users\vigopal\source\repos\P.910\src\Teleport_b_04_05_2025"

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
    # plt.legend(title="Question", bbox_to_anchor=(1.05, 1), loc='upper right')
    plt.grid(axis='y')
    plt.xticks(rotation=75)
    # plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # Step 1: Merge CSV files
    merged_df = merge_csv_files()
    
    # Step 2: Expand vote columns into "MOS"
    final_df = expand_votes_to_mos(merged_df)
    
    # Step 3: Save the final DataFrame to a new CSV file
    output_file = os.path.join(source_folder, "merged_and_expanded_votes.csv")
    final_df.to_csv(output_file, index=False)
    print(f"Final merged and expanded file saved to: {output_file}")
    
    # Step 4: Plot the average MOS with confidence interval
    plot_average_mos_with_ci(final_df)