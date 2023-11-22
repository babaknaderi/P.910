import csv
from urllib.parse import urljoin

from azure.storage.blob import ContainerClient

READ_SAS = '?sv=2023-01-03&ss=b&srt=o&st=2023-11-10T00%3A48%3A59Z&se=2023-12-01T00%3A48%3A00Z&sp=r&sig=ciM5X%2FQyTDiIj57uJPW7Oxrzhl%2BqBYu%2FyJzCpPhOkac%3D'

container_base_url = 'https://teleportvideo.blob.core.windows.net/subjective-runs/'
SAS = '?sv=2023-01-03&ss=b&srt=co&st=2023-11-10T00%3A40%3A29Z&se=2023-12-01T00%3A40%3A00Z&sp=rl&sig=t3%2F1VuF%2BovOHhWn49SQtQq68vQEGGw%2BzQM5hfpqelmY%3D'
container_client: ContainerClient = ContainerClient.from_container_url(container_base_url, credential=SAS)


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

        for blob in container_client.list_blobs(relative_path):
            if blob.name.endswith('.mp4') and keyword in blob.name:
                file = urljoin(container_base_url, blob.name) + READ_SAS
                if is_trap:
                    scale_p835_writer.writerow([file, get_answer(blob.name)])
                elif is_golden:
                    scale_p835_writer.writerow([file, gold_value])
                else:
                    scale_p835_writer.writerow([file])


csv_filename = r'C:\github\P.910\Teleport_1\configs\tlp_rating_clips_a.csv'
keyword = 'standalone'
relative_path = 'evaluations/11_20_2023/'
create_csv(csv_filename, keyword, relative_path)
#
# csv_filename = r'C:\github\P.910\Teleport\configs\tlp_rating_clips_b.csv'
# keyword = 'sidebyside'
# relative_path = 'evaluations/11_08_2023/'
# create_csv(csv_filename, keyword, relative_path)
#
# # Trapping
#
# csv_filename = r'C:\github\P.910\Teleport\configs\tlp_trapping_clips_a.csv'
# keyword = 'standalone'
# relative_path = 'trap/11_08_2023/'
# create_csv(csv_filename, keyword, relative_path, is_trap=True)
#
# csv_filename = r'C:\github\P.910\Teleport\configs\tlp_trapping_clips_b.csv'
# keyword = 'sidebyside'
# relative_path = 'trap/11_08_2023/'
# create_csv(csv_filename, keyword, relative_path, is_trap=True)


# csv_filename = r'C:\github\P.910\Teleport\configs\tlp_gold_clips_b_1.csv'
# keyword = 'sidebyside'
# relative_path = 'gold/11_08_2023/g1'
# create_csv(csv_filename, keyword, relative_path, is_golden=True, gold_value=1)
#
# csv_filename = r'C:\github\P.910\Teleport\configs\tlp_rating_clips_b_5.csv'
# keyword = 'sidebyside'
# relative_path = 'gold/11_08_2023/g5'
# create_csv(csv_filename, keyword, relative_path, is_golden=True, gold_value=5)
