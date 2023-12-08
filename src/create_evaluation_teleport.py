import os
from urllib.parse import urljoin

import pandas as pd
from azure.storage.blob import ContainerClient

TMP_FOLDER = r'C:\Temp\teleport'
RATING_SAS = ''
subjective_SAS = ''
cq_storage_SAS = ''

# config relative locations in container: https://teleportvideo.blob.core.windows.net/subjective-runs/
gold_relative_url = 'configs/master/12062023/tlp_gold_clips_b.csv'
tran_relative_url = 'configs/master/12062023/tlp_training_clips_b.csv'
trap_relative_url = 'configs/master/12062023/tlp_trapping_clips_b.csv'
config_relative_url = 'configs/master/12062023/master_b.cfg'

# blob clients
subjective_base_url = 'https://teleportvideo.blob.core.windows.net/subjective-runs/'
subjective_client = ContainerClient.from_container_url(subjective_base_url, credential=subjective_SAS)
cq_storage_base_url = 'https://cqstorageacct.blob.core.windows.net/teleport/'
cq_storage_client = ContainerClient.from_container_url(cq_storage_base_url, credential=cq_storage_SAS)

overwrite = False


def create_local_config(folder, relative_url, version):
    blob = subjective_client.get_blob_client(relative_url)
    filename = relative_url.split('/')[-1]
    download_path = os.path.join(folder, filename)
    with open(download_path, "wb+") as download_file:
        download_file.write(blob.download_blob().readall())
    subjective_client.get_blob_client('evaluations/' + version + '/mturk/configs/' + filename).upload_blob(
        open(download_path, 'rb'), overwrite=overwrite)


def create_local_csv_SAS(folder, relative_url, version):
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


def create_evaluation_folder(folder, input_csv, version, output_csv):
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
        subjective_client.get_blob_client(blob_name).start_copy_from_url(row['clip_url'] + cq_storage_SAS,
                                                                         requires_sync=True)

        eval_csv = pd.concat([eval_csv, pd.DataFrame({'pvs': [urljoin(subjective_base_url, blob_name) + RATING_SAS]})])

    output_csv_file = os.path.join(folder, output_csv)
    eval_csv.to_csv(output_csv_file, index=False)
    subjective_client.get_blob_client('evaluations/' + version + '/mturk/configs/' + output_csv).upload_blob(
        open(output_csv_file, 'rb'), overwrite=overwrite)


def create_evaluation(rating_source, version, output_csv):
    folder = os.path.dirname(rating_source)
    rating_file = os.path.basename(rating_source)

    create_local_csv_SAS(folder, gold_relative_url, version)
    create_local_csv_SAS(folder, tran_relative_url, version)
    create_local_csv_SAS(folder, trap_relative_url, version)
    create_local_config(folder, config_relative_url, version)

    create_evaluation_folder(folder, rating_file, version, output_csv)


def merge_clips_into_side_by_side(merge_csv_file):
    data = pd.read_csv(merge_csv_file)
    rating_source = pd.DataFrame(columns=['model', 'type', 'clip_url'])
    # create temp folder
    if not os.path.exists(TMP_FOLDER):
        os.makedirs(TMP_FOLDER)
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

            command_to_run = 'ffmpeg -y -i {0} -i {1} -filter_complex [0:v][1:v]hstack=inputs=2[0_v]; -map "[0_v]" -map 0:a -c:a copy  {2}'.format(
                avatar_path, real_path, merged)

            print('Running: ' + command_to_run)
            os.system(command_to_run)

            print('Processed: ' + merged)
            # upload merged file
            blob_name = merged.split('\\')[-1]
            blob_name = row['cq_path'] + '/' + blob_name
            cq_storage_client.get_blob_client(blob_name).upload_blob(open(merged, 'rb'), overwrite=overwrite)
            print('Uploaded: ' + blob_name)
            # add to rating_source
            rating_source = pd.concat(
                [rating_source,
                 pd.DataFrame({'model': [model], 'type': [type], 'clip_url': [cq_storage_base_url + + blob_name]})])
        except Exception as e:
            print(e)
    # delete temp files
    for file in os.listdir(TMP_FOLDER):
        os.remove(os.path.join(TMP_FOLDER, file))

    rating_source.to_csv(merge_csv_file.replace('.csv', '_side_by_side.csv'), index=False)


csv_file = r'C:\github\P.910\new_study\rating_source_b.csv'
eval_ver = '12_07_2023'
csv_output = 'tlp_rating_clips_b.csv'
create_evaluation(csv_file, eval_ver, csv_output)

## merge clips side by side for subjective evaluation using a csv file
## model: model name
## type: speak/standalone, speak/sidebyside, expression/standalone, expression/sidebyside
## cq_path: cq storage path to write the merged clip
## clip_avatar: avatar clip url in cq storage
## clip_real: real clip url in cq storage
## example of csv file: https://teleportvideo.blob.core.windows.net/subjective-runs/configs/master/12062023/merge_clips.csv

# merge_clips_into_side_by_side(r'C:\github\P.910\merg\merge_clips.csv')
