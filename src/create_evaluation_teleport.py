import os
from urllib.parse import urljoin

import pandas as pd
from azure.storage.blob import ContainerClient, ContentSettings
from enum import Enum
import tempfile
import uuid

class Template(Enum):
    TEMPLATE_A = "1"
    TEMPLATE_B = "2"
    TEMPLATE_PROBLEM_TOKEN = "3" # this is basically similar to template B filewise

TMP_FOLDER = r'C:\Temp\teleport'
RATING_SAS = '' # avoid using SAS tokens for rating clips, rather use public_storage, clips will be copied there.
subjective_SAS = r''
cq_storage_SAS = r''
teleport_westus3_SAS = r''  # SAS for teleport westus3 storage, used for copying clips to public storage
public_storage_SAS = r''

# config relative locations in container: https://teleportvideo.blob.core.windows.net/subjective-runs/
# DO NOT update these vaulue but the one in set_config_relative_urls function
gold_relative_url = None
tran_relative_url = None
trap_relative_url = None
config_relative_url = None

# blob clients
subjective_base_url = 'https://teleportvideo.blob.core.windows.net/subjective-runs/'
subjective_client = ContainerClient.from_container_url(subjective_base_url, credential=subjective_SAS)
cq_storage_base_url = 'https://cqstorageacct.blob.core.windows.net/teleport/'
cq_storage_client = ContainerClient.from_container_url(cq_storage_base_url, credential=cq_storage_SAS)
teleport_westus3_base_url = 'https://teleportstorageacctwus3.blob.core.windows.net/experiments/'
teleport_westus3_client = ContainerClient.from_container_url(teleport_westus3_base_url, credential=teleport_westus3_SAS)
public_storage_base_url = 'https://mlvideopub.blob.core.windows.net/teleportvideo/'
public_storage_client = ContainerClient.from_container_url(public_storage_base_url, credential=public_storage_SAS)

overwrite = False

def set_config_relative_urls(template):
    global gold_relative_url, tran_relative_url, trap_relative_url, config_relative_url
    if template == Template.TEMPLATE_A:
        # template A, links are copied from study 10_14_2024. beside the master_a.cfg other files are not updated
        gold_relative_url = 'configs/master/03192025/tlp_gold_clips_a.csv'
        tran_relative_url = 'configs/master/03192025/tlp_training_clips_a.csv'
        trap_relative_url = 'configs/master/03192025/tlp_trapping_clips_a.csv'
        config_relative_url = 'configs/master/03192025/master_a.cfg'
    elif template == Template.TEMPLATE_B:
        # Template B
        gold_relative_url = 'configs/master/03192025/tlp_gold_clips_b.csv'
        tran_relative_url = 'configs/master/03192025/tlp_training_clips_b.csv'
        trap_relative_url = 'configs/master/03192025/tlp_trapping_clips_b.csv'
        config_relative_url = 'configs/master/03192025/master_b.cfg'    
    elif template == Template.TEMPLATE_PROBLEM_TOKEN:
        # Template problem Token
        gold_relative_url = 'configs/master/03192025/tlp_gold_clips_pt.csv'
        tran_relative_url = 'configs/master/03192025/tlp_training_clips_pt.csv'
        trap_relative_url = 'configs/master/03192025/tlp_trapping_clips_pt.csv'
        config_relative_url = 'configs/master/03192025/master_pt.cfg'

def create_local_config(folder, relative_url, version):
    blob = subjective_client.get_blob_client(relative_url)
    filename = relative_url.split('/')[-1]
    download_path = os.path.join(folder, filename)
    with open(download_path, "wb+") as download_file:
        download_file.write(blob.download_blob().readall())
    subjective_client.get_blob_client('evaluations/' + version + '/mturk/configs/' + filename).upload_blob(
        open(download_path, 'rb'), overwrite=overwrite)


def create_local_csv_SAS(folder, relative_url, version):
    print('downloading: ' + relative_url)
    blob = subjective_client.get_blob_client(relative_url)
    csv_filename = relative_url.split('/')[-1]
    download_path = os.path.join(folder, csv_filename)
    with open(download_path, "wb+") as download_file:
        download_file.write(blob.download_blob().readall())
    data = pd.read_csv(download_path)
    column_names = data.columns.where(data.columns.str.endswith('pvs')).dropna()
    if len(column_names) > 1:
        raise Exception('multiple pvs columns found')
    elif len(column_names) == 1:
        column_name = column_names[0]
        data[column_name] = data[column_name].apply(lambda x: x + RATING_SAS)
    data.to_csv(download_path, index=False)
    subjective_client.get_blob_client('evaluations/' + version + '/mturk/configs/' + csv_filename).upload_blob(
        open(download_path, 'rb'), overwrite=overwrite)


def create_evaluation_folder(folder, input_csv, version, output_csv, strip_audio=False):
    input_csv_path = os.path.join(folder, input_csv)
    subjective_client.get_blob_client(
        'evaluations/' + version + '/mturk/configs/' + input_csv).upload_blob(
        open(input_csv_path, 'rb'), overwrite=overwrite)
    data = pd.read_csv(input_csv_path)

    eval_csv = pd.DataFrame(columns=['pvs'])
    for index, row in data.iterrows():
        blob_name = row['clip_url'].split('/')[-1]
        print(f'processing {blob_name}:')
        blob_name = 'evaluations/' + version + '/' + row['type'] + '/' + row['model'] + '/' + blob_name
        
        # Set content type to video/mp4 when copying
        content_settings = ContentSettings(content_type='video/mp4')
        
        if strip_audio:
            # Download the video, strip audio, then upload
            temp_dir = os.path.join(TMP_FOLDER, str(uuid.uuid4()))
            os.makedirs(temp_dir, exist_ok=True)
            
            # Download original video
            original_path = os.path.join(temp_dir, os.path.basename(blob_name))
            if row['clip_url'].startswith(teleport_westus3_base_url):
                source_blob_url = row['clip_url'] + teleport_westus3_SAS
            else:
                source_blob_url = row['clip_url'] + cq_storage_SAS
            
            # Get the blob client for the source blob
            source_container_name = row['clip_url'].split('/')[3]
            source_blob_name = '/'.join(row['clip_url'].split('/')[4:])
            
            # Download the blob to local file
            with open(original_path, 'wb') as file:
                if row['clip_url'].startswith(teleport_westus3_base_url):
                    blob_client = teleport_westus3_client.get_blob_client(source_blob_name)
                else:
                    blob_client = cq_storage_client.get_blob_client(source_blob_name)
                download_data = blob_client.download_blob()
                file.write(download_data.readall())
            
            # Strip audio
            no_audio_path = strip_audio_from_video(original_path)
            
            # Upload processed video
            with open(no_audio_path, 'rb') as file:
                subjective_client.get_blob_client(blob_name).upload_blob(
                    file, 
                    overwrite=overwrite,
                    content_settings=content_settings
                )
                public_storage_client.get_blob_client(blob_name).upload_blob(
                    open(no_audio_path, 'rb'), 
                    overwrite=overwrite,
                    content_settings=content_settings
                )
            
            # Clean up temporary files
            os.remove(original_path)
            os.remove(no_audio_path)
            os.rmdir(temp_dir)
        else:
            # Original implementation - copy directly
            storage_sas = cq_storage_SAS if not row['clip_url'].startswith(teleport_westus3_base_url) else teleport_westus3_SAS
            subjective_client.get_blob_client(blob_name).start_copy_from_url(
                row['clip_url'] + storage_sas,
                requires_sync=True
            )
            subjective_client.get_blob_client(blob_name).set_http_headers(content_settings=content_settings)
            
            public_storage_client.get_blob_client(blob_name).start_copy_from_url(
                row['clip_url'] + storage_sas,
                requires_sync=True
            )
            public_storage_client.get_blob_client(blob_name).set_http_headers(content_settings=content_settings)
        
        eval_csv = pd.concat([eval_csv, pd.DataFrame({'pvs': [urljoin(public_storage_base_url, blob_name) + RATING_SAS]})])

    output_csv_file = os.path.join(folder, output_csv)
    eval_csv.to_csv(output_csv_file, index=False)
    print(output_csv)
    subjective_client.get_blob_client('evaluations/' + version + '/mturk/configs/' + output_csv).upload_blob(
        open(output_csv_file, 'rb'), overwrite=overwrite)


def create_evaluation(rating_source, version, output_csv, strip_audio=False):
    folder = os.path.dirname(rating_source)
    rating_file = os.path.basename(rating_source)

    create_local_csv_SAS(folder, gold_relative_url, version)
    create_local_csv_SAS(folder, tran_relative_url, version)
    create_local_csv_SAS(folder, trap_relative_url, version)
    create_local_config(folder, config_relative_url, version)

    create_evaluation_folder(folder, rating_file, version, output_csv, strip_audio)


def merge_clips_into_side_by_side(merge_csv_file, strip_audio=False):
    data = pd.read_csv(merge_csv_file)
    rating_source = pd.DataFrame(columns=['model', 'type', 'clip_url'])
    # create temp folder
    if not os.path.exists(TMP_FOLDER):
        os.makedirs(TMP_FOLDER)
    TARGET_HEIGHT = 720  # common height for hstack; adjust if needed
    for index, row in data.iterrows():
        try:
            model = row['model']  # model	type	clip_avatar	clip_real
            type = row['type']
            clip_avatar = row['clip_avatar']
            clip_real = row['clip_real']
            avatar_path = os.path.join(TMP_FOLDER, clip_avatar.split('/')[-1])
            print('Downloading: ' + clip_avatar)
            with open(avatar_path, "wb+") as download_file:
                blob_location = clip_avatar.replace(cq_storage_base_url, '')
                avatar = cq_storage_client.get_blob_client(blob_location).download_blob().readall()
                download_file.write(avatar)
            real_path = os.path.join(TMP_FOLDER, clip_real.split('/')[-1])
            print('Downloading: ' + clip_real)
            with open(real_path, "wb+") as download_file:
                blob_location = clip_real.replace(cq_storage_base_url, '')
                real = cq_storage_client.get_blob_client(blob_location).download_blob().readall()
                download_file.write(real)

            # add suffix 'merged' to avatar file
            merged = avatar_path.replace('.mp4', '_merged.mp4')

            # Build a filter that forces both inputs to the same height and stacks them
            vf = (
                f'[0:v]scale=-2:{TARGET_HEIGHT},setpts=PTS-STARTPTS[v0];'
                f'[1:v]scale=-2:{TARGET_HEIGHT},setpts=PTS-STARTPTS[v1];'
                f'[v0][v1]hstack=inputs=2,format=yuv420p[v]'
            )

            # Command to merge videos side-by-side
            if strip_audio:
                command_to_run = (
                    'ffmpeg -y -i {0} -i {1} -filter_complex '
                    '"[0:v]scale=-1:ih[vid1];[1:v]scale=-1:ih[vid2];[vid1][vid2]hstack=inputs=2[v]" '
                    '-map "[v]" -c:v libx264 -crf 17 -preset slow -an {2}'
                ).format(avatar_path, real_path, merged)
            else:
                command_to_run = (
                    'ffmpeg -y -i {0} -i {1} -filter_complex '
                    '"[0:v]scale=-1:ih[vid1];[1:v]scale=-1:ih[vid2];[vid1][vid2]hstack=inputs=2[v]" '
                    '-map "[v]" -c:v libx264 -crf 17 -preset slow -map 0:a -c:a copy {2}'
                ).format(avatar_path, real_path, merged)

            print('Running: ' + command_to_run)
            os.system(command_to_run)

            print('Processed: ' + merged)
            # upload merged file with proper content type
            blob_name = merged.split('\\')[-1]
            blob_name = row['cq_path'] + '/' + blob_name
            content_settings = ContentSettings(content_type='video/mp4')
            cq_storage_client.get_blob_client(blob_name).upload_blob(
                open(merged, 'rb'), 
                overwrite=overwrite,
                content_settings=content_settings
            )
            print('Uploaded: ' + blob_name)
            # add to rating_source
            rating_source = pd.concat(
                [rating_source,
                 pd.DataFrame({'model': [model], 'type': [type], 'clip_url': [cq_storage_base_url + blob_name]})])
        except Exception as e:
            print(e)
    # delete temp files
    for file in os.listdir(TMP_FOLDER):
        os.remove(os.path.join(TMP_FOLDER, file))

    rating_source.to_csv(merge_csv_file.replace('.csv', '_side_by_side.csv'), index=False)


# Helper function to check and update content type of existing blobs
def ensure_video_content_type(container_client, prefix=''):
    """
    Check all blobs in a container with a given prefix and set content-type to video/mp4 for mp4 files
    """
    content_settings = ContentSettings(content_type='video/mp4')
    count = 0
    
    for blob in container_client.list_blobs(name_starts_with=prefix):
        if blob.name.lower().endswith('.mp4'):
            if blob.content_settings.content_type != 'video/mp4':
                print(f"Updating content type for {blob.name}")
                container_client.get_blob_client(blob.name).set_http_headers(content_settings=content_settings)
                count += 1
    
    print(f"Updated content type for {count} blobs")

# Uncomment to run content type check/update on existing blobs
# ensure_video_content_type(subjective_client, 'evaluations/')
# ensure_video_content_type(public_storage_client, 'evaluations/')

# Helper function to strip audio from a video file
def strip_audio_from_video(input_path, output_path=None):
    """
    Strip audio from a video file using FFmpeg
    
    Args:
        input_path: Path to input video file
        output_path: Path to output video file. If None, a temporary file is created.
        
    Returns:
        Path to the output file
    """
    if output_path is None:
        # Create temporary file in the same directory as input
        output_dir = os.path.dirname(input_path)
        temp_filename = f"{uuid.uuid4().hex}.mp4"
        output_path = os.path.join(output_dir, temp_filename)
    
    # # Use FFmpeg to strip audio
    # command = f'ffmpeg -i "{input_path}" -c:v copy -an "{output_path}" -y'
    # Use FFmpeg to strip audio and set CRF to 17 for high quality
    # Instead of -c:v copy which just copies the video stream without re-encoding
    # we now use -c:v libx264 -crf 17 to ensure high quality output
    command = f'ffmpeg -i "{input_path}" -c:v libx264 -crf 17 -an "{output_path}" -y'
    print(f"Stripping audio: {command}")
    os.system(command)
    
    return output_path

csv_file = r'C:\Users\vigopal\source\repos\P.910\src\09_24_2025\rating_source_noaudio.csv'
template = Template.TEMPLATE_PROBLEM_TOKEN
eval_ver = '09_24_2025_noaudio'
csv_output = 'rating_source_noaudio_pt.csv'
# Set to True to strip audio from all videos
strip_audio = True
set_config_relative_urls(template)
create_evaluation(csv_file, eval_ver, csv_output, strip_audio)

## merge clips side by side for subjective evaluation using a csv file
## model: model name
## type: speak/standalone, speak/sidebyside, expression/standalone, expression/sidebyside
## cq_path: cq storage path to write the merged clip
## clip_avatar: avatar clip url in cq storage
## clip_real: real clip url in cq storage
## example of csv file: https://teleportvideo.blob.core.windows.net/subjective-runs/configs/master/12062023/merge_clips.csv

# merge_clips_into_side_by_side(r'C:\github\P.910\merg\merge_clips.csv', strip_audio=False)
