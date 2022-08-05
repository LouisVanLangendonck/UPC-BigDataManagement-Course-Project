from genericpath import exists
from xmlrpc.client import boolean
import pymongo as pm
import json
import datetime
import pandas as pd
import argparse
import paramiko
from hdfs import InsecureClient
from os.path import exists

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--host_ip",
                        help="host ip of virtual machine / mongo server",
                        type=str, default = '10.4.41.59')
    parser.add_argument("-d", "--hdfs_port",
                        help="hdfs port of virtual machine",
                        type=int, default = 9870)
    parser.add_argument("-m", "--mongo_port",
                        help="mongo port of mongodb instance in virtual machine",
                        type=int, default = 27017)                    
    parser.add_argument("-u", "--vm_user",
                        help="username to log into virtual machine",
                        type=str, default = 'bdm')
    parser.add_argument("-p", "--vm_pass",
                        help="password to log into virtual machine",
                        type=str, default = 'root')
    parser.add_argument("-n", "--name_pandas_db", 
                        help="Name of the pandas database",
                        type=str, default='P1')
    parser.add_argument("-a", "--allow_metadata",
                        help="Wheather or not to work with a seperate metadata file keeping track of which data has already been added to mongoDB.",
                        type=str, default='True')         
    return parser


def bulk_transfer_csv(hdfs_client, hdfs_file_path, mongo_database, collection_name):
    with hdfs_client.read(hdfs_file_path) as reader:
        df = pd.read_csv(reader)
    json_data = json.loads(df.to_json(orient='records'))
    db_to_load = mongo_database[collection_name]
    res = db_to_load.insert_many(json_data)
    print('{} rows inserted from {} to collection {}'.format(len(res.inserted_ids), hdfs_file_path, collection_name))


def bulk_transfer_json(hdfs_client, hdfs_file_path, mongo_database, collection_name):
    with hdfs_client.read('/user/temporary/idealista/2022-03-31/2020_01_08_idealista.json', encoding='utf-8') as reader:
        json_data = json.load(reader)    
    if collection_name == 'idealista':
        json_data = timestamp_idealista(json_data, hdfs_file_path)
    db_to_load = mongo_database[collection_name]
    res = db_to_load.insert_many(json_data)
    print('{} rows inserted from {} to collection {}'.format(len(res.inserted_ids), hdfs_file_path, collection_name))


def timestamp_idealista(json_data, file_path):
    file_name = file_path.split('/')[-1]
    date = datetime.datetime.strptime('-'.join(file_name.split('_')[0:3]), '%Y-%m-%d')
    formatted_datetime = date.isoformat()
    for i in range(len(json_data)):
        json_data[i]['scrape_date'] = formatted_datetime
    return json_data


def already_transfered(date, source):
        with open('transfer_metadata.json', 'w') as f:
            metadata=json.load(f)
        if source in metadata:
            if date in metadata[source]:
                return True
            else:
                return False
        else:
            return False


def metadata_exists(filepath):
    return exists(filepath)


def create_empty_metadata(filepath):
    print("metadata file keeping track of data transfers not yet created")
    print("creating transfer metadata file..")
    with open(filepath, 'w') as f:
            metadata = {}
            json.dump(metadata, f)


def write_to_metadate(filepath, source, date):
    with open(filepath, 'r+') as f:
            metadata=json.load(f)
    metadata.setdefault(source, []).append(date)
    metadata[source] = list(set(metadata[source]))
    with open(filepath, "w") as f:
        json.dump(metadata, f)


def pipeline(host_ip, hdfs_port, mongo_port, vm_user, vm_pass, name_pandas_db, allow_metadata):
    print('----------------------------------------------------------')
    print('Connecting to Virtual Machine...')
    vm_client = paramiko.SSHClient()
    vm_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    vm_client.connect(host_ip, username=vm_user, password=vm_pass)

    print('----------------------------------------------------------')
    print('Connecting to hdfs...')
    hdfs_client = InsecureClient('http://{}:{}/'.format(host_ip, hdfs_port), user=vm_user)

    print('----------------------------------------------------------')
    print('Scanning for new files in the temporal landing zone...')
    if not metadata_exists("transfer_metadata.json"):
        create_empty_metadata("transfer_metadata.json")
    
    files_to_transfer = {}
    for src in hdfs_client.list('/user/temporary/'):
            for dt in hdfs_client.list('/user/temporary/{}'.format(src)):
                if hdfs_client.status('/user/temporary/{}/{}'.format(src,dt))['childrenNum'] == 0:
                    pass
                elif allow_metadata == 'True':
                    if not already_transfered(date=dt, source=src):
                        files_to_transfer.setdefault(src, []).append(dt)
                else:
                    files_to_transfer.setdefault(src, []).append(dt)
    
    if len(files_to_transfer) == 0:
        print('----------------------------------------------------------')
        print('All files in the HDFS already transfered to mongoDB!')
        print('Program shutting down...')
    
    else:
        print('----------------------------------------------------------')
        print('New files found in the HDFS!')
        print('Preparing transfer(s)...')

        print('----------------------------------------------------------')
        print('Starting MongoDB on Virtual Machine...')
        command = '~/BDM_Software/mongodb/bin/mongod --bind_ip_all --dbpath /home/bdm/BDM_Software/data/mongodb_data/'
        stdin,stdout,stderr=vm_client.exec_command(command)
        print(stdout.readlines())
        print(stderr.readlines())
        
        print('----------------------------------------------------------')
        print('Connecting to MongoDB...')
        mongo_client = pm.MongoClient(host=host_ip, port=mongo_port, connect=True)
        if name_pandas_db not in mongo_client.list_database_names():
            print('"{}" does not yet exist in mongoDB.'.format(name_pandas_db))
            print('Creating and connecting to "{}"...'.format(name_pandas_db))
        else:
            print('"{}" already exist in mongoDB.'.format(name_pandas_db))
            print('Connecting to "{}"...'.format(name_pandas_db))
        db = mongo_client[name_pandas_db]
        
        for src, dts in files_to_transfer.items():
            if src in hdfs_client.list('/user/temporary/'):
                for dt in dts:
                        if dt in hdfs_client.list('/user/temporary/{}'.format(src)):
                            print('Loading in file(s) from {} with scrape date {}:'.format(src, dt))
                            for file in hdfs_client.list('/user/temporary/{}/{}'.format(src, dt)):
                                if src == 'idealista':
                                    bulk_transfer_json(hdfs_client=hdfs_client, hdfs_file_path='/user/temporary/{}/{}/{}'.format(src, dt, file), 
                                    mongo_database=db, collection_name='idealista')
                                elif src == 'covid':
                                    bulk_transfer_json(hdfs_client=hdfs_client, hdfs_file_path='/user/temporary/{}/{}/{}'.format(src, dt, file), 
                                    mongo_database=db, collection_name='covid')
                                elif src == 'opendatabcn-income':
                                    bulk_transfer_csv(hdfs_client=hdfs_client, hdfs_file_path='/user/temporary/{}/{}/{}'.format(src, dt, file), 
                                    mongo_database=db, collection_name='opendata')
                                elif src == 'lookup_tables':
                                    bulk_transfer_csv(hdfs_client=hdfs_client, hdfs_file_path='/user/temporary/{}/{}/{}'.format(src, dt, file), 
                                    mongo_database=db, collection_name='lookup')
                                if allow_metadata == 'True':
                                    write_to_metadate(filepath="transfer_metadata.json", source=src, date=dt)
                        else:
                                print('New source detected or source changed name...')


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    pipeline(args.host_ip, args.hdfs_port, args.mongo_port, args.vm_user, args.vm_pass, args.name_pandas_db, args.allow_metadata)