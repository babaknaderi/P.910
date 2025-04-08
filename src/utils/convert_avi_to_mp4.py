import os
import subprocess

# Define source and destination directories
source_folder = r"C:\Teleport\Evaluations\Make_your_anchor\inference_mesh_v15_10801x1_trained_models_v15_10801x1_L8_10801x1_EP360_v2\merged_videos_ratings_03252025"
destination_folder = source_folder  # Output in the same folder

# Ensure FFmpeg is installed
def check_ffmpeg():
    try:
        subprocess.run(["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    except FileNotFoundError:
        print("FFmpeg is not installed. Please install it and ensure it's in your PATH.")
        exit(1)

# Convert .avi to .mp4
def convert_videos():
    for file_name in os.listdir(source_folder):
        if file_name.endswith(".avi"):
            input_path = os.path.join(source_folder, file_name)
            output_path = os.path.join(destination_folder, file_name.replace(".avi", ".mp4"))
            
            # FFmpeg command
            command = [
                "ffmpeg",
                "-i", input_path,  # Input file
                "-c:v", "libx264",  # Video codec
                "-preset", "fast",  # Encoding speed
                "-crf", "23",  # Quality (lower is better)
                "-c:a", "aac",  # Audio codec
                "-b:a", "128k",  # Audio bitrate
                output_path
            ]
            
            print(f"Converting: {file_name} -> {os.path.basename(output_path)}")
            subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(f"Finished: {output_path}")

if __name__ == "__main__":
    check_ffmpeg()
    convert_videos()