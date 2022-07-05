"""
Searching high resolution images based on img2dataset downloaded resource files.

A typical node structure:
node1/00000.tar
...
node1/2xxxx.tar

--------

A typical tar folder after extracting:
node1/00000/000000001.jpg
node1/00000/000000001.json
node1/00000/000000001.txt
...
node1/2xxxx/2xxxx9999.jpg
node1/2xxxx/2xxxx9999.json
node1/2xxxx/2xxxx9999.txt
"""

from multiprocessing.pool import ThreadPool
import os
import fsspec
import json
from datetime import datetime

size = 1024


def get_relative_path(tar_dir, img_path, base_path):
    return os.path.relpath(tar_dir + '/' + img_path, base_path)


def filter(data):
    return data.get('WIDTH') >= size and data.get('HEIGHT') >= size


def search_dir(tar_dir):
    fs, url_path = fsspec.core.url_to_fs(tar_dir)
    files = fs.glob(url_path + '/*.json')
    records = []

    for file in files:
        with open(file) as f:
            data = json.load(f)
            if filter(data):
                records.append(os.path.basename(
                    f.name).replace('.json', '.jpg'))

    return (tar_dir, records)


def main(folder_list, output_file):
    fs, log_path = fsspec.core.url_to_fs(output_file)
    if fs.exists(log_path):
        print(
            f"Output file {log_path} already exists, specify it with a new path")
        return

    log_file = open(log_path, 'w')

    for folder in folder_list:
        fs, url_path = fsspec.core.url_to_fs(folder)
        if not fs.exists(url_path):
            print(f"Folder path not exists: {url_path}")
            continue

        base_path = url_path + '/..'
        tars = fs.glob(url_path + '/*.tar')
        dirs = list(map(lambda x: x.replace('.tar', ''),  tars))
        print(f"Found {len(dirs)} tar files in {url_path}, processing now ...")
        start = datetime.now()
        with ThreadPool(32) as thread_pool:
            for tar_dir, records in thread_pool.imap_unordered(search_dir, dirs):
                if len(records) == 0:
                    continue

                log_file.write('\n'.join(
                    list(map(lambda x: get_relative_path(tar_dir, x, base_path), records))) + '\n')
        print(f"Elapsed time: {datetime.now() - start}")

    log_file.close()


node_list = ['../node1', '../node2', '../node3']
main(node_list, '../1026.txt')
