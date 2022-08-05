from hdfs import InsecureClient
import datetime
import zipfile
import os
import pandas as pd
import json
import argparse


def get_parser():
    p = argparse.ArgumentParser()
    p.add_argument('-i', '--input', help='Location of the input zip file', type=str, default='')
    p.add_argument('-z', '--unzip_loc', help="Location of the where the zip file gets unzipped. "
                                             "By default equal to the input path", type=str, default='')
    p.add_argument('-a', '--hdfsaddress', help='IP-Address of the HDFS', type=str, default="10.4.41.59")
    p.add_argument('-p', '--hdfsport', help='Port of the HDFS', type=str, default=9870)
    p.add_argument('-u', '--uhdfs', help='Username of the HDFS', type=str, default='bdm')
    return p


def check_if_hdfs_dir_exists(client, path):
    if client.content(path, strict=False) is None:
        return False
    else:
        return True


def unzip_files(path, path_to=''):
    if path_to == '':
        path_to = path
        path_to = path_to.replace(path.split("/")[-1], "")
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(path_to)
    return path_to


def find_files_in_path(path):
    return os.listdir(path)


def get_hdfs_client(ip, port, user):
    return InsecureClient(f'http://{ip}:{port}/', user=user)


def delete_zip_file(data_path):
    os.remove(data_path)


def insert_zip_in_hdfs(data_path, unzip_path, hdfs_client):
    path = unzip_files(data_path, unzip_path)
    delete_zip_file(data_path)
    print(path)
    print("_" * 100)
    print('Transferring files in the specified folder to the HDFS... \n')
    folders = find_files_in_path(path)
    
    date = datetime.date.today()
    if not check_if_hdfs_dir_exists(hdfs_client, f'/user/temporary'):
        hdfs_client.makedirs('/user/temporary')

    for folder in folders:
        print("_" * 100)
        print(f"Checking if /temporary/{folder} exists...")
        if not check_if_hdfs_dir_exists(hdfs_client, f'temporary/{folder}'):
            print(f"/temporary/{folder} does not exist. Creating the dir...")
            hdfs_client.makedirs(f'temporary/{folder}')
            print("Success! \n")
        else:
            print(f"/temporary/{folder} exists \n")

        print("." * 100)
        print(f"Checking if /temporary/{folder}/{date} exists...")
        if not check_if_hdfs_dir_exists(hdfs_client, f'temporary/{folder}/{date}'):
            print(f"/temporary/{folder}/{date} does not exist. Creating the dir...")
            hdfs_client.makedirs(f'temporary/{folder}/{date}')
            print("Success! \n")
        else:
            print(f"/temporary/{folder}/{date} exists")
            print("")

        print("." * 100)
        print(f"Loading files from {folder} to hdfs dir: /user/temporary/{folder}/{date}...")

        fs = find_files_in_path(path + f'/{folder}')

        for f in fs:
            if ".csv" in f:
                with hdfs_client.write(f'/user/temporary/{folder}/{date}/{f}', encoding='utf-8') as writer:
                    pd.read_csv(path + f'/{folder}/{f}').to_csv(writer)

            elif ".json" in f:
                with open(path + f'/{folder}/{f}', 'r') as read_file:
                    json_file = json.load(read_file)
                with hdfs_client.write(f'/user/temporary/{folder}/{date}/{f}.json', encoding='utf-8') as writer:
                    json.dump(json_file, writer)
            else:
                print("file format not supported... :(")

        print('All files were successfully loaded! \n')


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    client = get_hdfs_client(ip=args.hdfsaddress, port=args.hdfsport, user=args.uhdfs)

    insert_zip_in_hdfs(args.input, args.unzip_loc, client)
