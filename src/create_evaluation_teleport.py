import os
from urllib.parse import urljoin

import pandas as pd
from azure.storage.blob import ContainerClient

RATING_SAS = ''
subjective_SAS = ''
cq_storage_SAS = ''

# config locations
gold_relative_url = 'configs/master/12012023/tlp_gold_clips_a.csv'
tran_relative_url = 'configs/master/12012023/tlp_training_clips_a.csv'
trap_relative_url = 'configs/master/12012023/tlp_trapping_clips_a.csv'
config_relative_url = 'configs/master/12012023/master_a.cfg'
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


def create_evaluation_folder(folder, input_csv, version, output_csv='tlp_rating_clips_a.csv'):
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

    subjective_client.get_blob_client('evaluations/' + version + '/mturk/configs/' + output_csv).upload_blob(
        open(output_csv, 'rb'), overwrite=overwrite)
    output_csv = os.path.join(folder, output_csv)
    eval_csv.to_csv(output_csv, index=False)


def create_evaluation(rating_source, version):
    folder = os.path.dirname(rating_source)
    rating_file = os.path.basename(rating_source)

    create_local_csv_SAS(folder, gold_relative_url, version)
    create_local_csv_SAS(folder, tran_relative_url, version)
    create_local_csv_SAS(folder, trap_relative_url, version)
    create_local_config(folder, config_relative_url, version)

    create_evaluation_folder(folder, rating_file, version, output_csv='tlp_rating_clips_a.csv')


csv_file = r'C:\github\P.910\new_study\rating_source.csv'
eval_ver = '12_04_2023'
create_evaluation(csv_file, eval_ver)
