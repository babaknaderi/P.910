import csv
import glob
import os
from urllib.parse import urljoin

from azure.storage.blob import ContainerClient

subjective_SAS = ""
subjective_base_url = 'https://teleportvideo.blob.core.windows.net/subjective-runs/'
subjective_client = ContainerClient.from_container_url(subjective_base_url, credential=subjective_SAS)


def rename_files():
    # rename local files by adding prefix using the folder name
    folder = r'C:\Users\ashkana\Downloads\P3D\individual_videos'
    for file in glob.glob(folder + '/**/*.mp4'):
        file_name = file.split('\\')[-1]
        folder_name = file.split('\\')[-2]
        folder_location = file.replace(file_name, '')
        new_name = 'ind_' + folder_name + '_' + file_name
        new_file = os.path.join(folder_location, new_name)
        # rename file
        os.rename(file, new_file)
        print(new_file)


def get_answer(filename):
    return filename.split('_')[-1].split('.')[0]


def create_csv(csv_filename, keyword, relative_path, is_trap=False, is_golden=False, gold_value=1):
    all_files = []

    with open(csv_filename, 'w', newline='') as csvfile:
        scale_p835_writer = csv.writer(csvfile)
        if is_trap:
            scale_p835_writer.writerow(['trapping_pvs', 'trapping_ans'])
        elif is_golden:
            scale_p835_writer.writerow(['gold_clips_pvs', 'gold_clips_ans'])
        else:
            scale_p835_writer.writerow(['pvs'])

        for blob in subjective_client.list_blobs(relative_path):
            if blob.name.endswith('.mp4') and keyword in blob.name:
                file = urljoin(subjective_base_url, blob.name) + RATING_SAS
                if is_trap:
                    scale_p835_writer.writerow([file, get_answer(blob.name)])
                elif is_golden:
                    scale_p835_writer.writerow([file, gold_value])
                else:
                    scale_p835_writer.writerow([file])

# create Rating, Trapping and Golden csv files using folder and keyword
# Rating
# template a
# csv_filename = r'C:\github\P.910\Teleport_1\configs\tlp_rating_clips_a.csv'
# keyword = 'standalone'
# relative_path = 'evaluations/11_20_2023/'
# create_csv(csv_filename, keyword, relative_path)
# # template b
# csv_filename = r'C:\github\P.910\Teleport\configs\tlp_rating_clips_b.csv'
# keyword = 'sidebyside'
# relative_path = 'evaluations/11_08_2023/'
# create_csv(csv_filename, keyword, relative_path)
#
# # Trapping
# template a
# csv_filename = r'C:\github\P.910\Teleport\configs\tlp_trapping_clips_a.csv'
# keyword = 'standalone'
# relative_path = 'trap/11_08_2023/'
# create_csv(csv_filename, keyword, relative_path, is_trap=True)
# # template b
# csv_filename = r'C:\github\P.910\Teleport\configs\tlp_trapping_clips_b.csv'
# keyword = 'sidebyside'
# relative_path = 'trap/11_08_2023/'
# create_csv(csv_filename, keyword, relative_path, is_trap=True)

# # Golden
# template a
# csv_filename = r'C:\github\P.910\Teleport\configs\tlp_gold_clips_b_1.csv'
# keyword = 'sidebyside'
# relative_path = 'gold/11_08_2023/g1'
# create_csv(csv_filename, keyword, relative_path, is_golden=True, gold_value=1)
# # template b
# csv_filename = r'C:\github\P.910\Teleport\configs\tlp_rating_clips_b_5.csv'
# keyword = 'sidebyside'
# relative_path = 'gold/11_08_2023/g5'
# create_csv(csv_filename, keyword, relative_path, is_golden=True, gold_value=5)
