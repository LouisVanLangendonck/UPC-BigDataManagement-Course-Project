import fabric
import paramiko
import datetime

class VM():
    def __init__(self,
                 hostname = "10.4.41.59",
                 port = 22,
                 username = "bdm",
                 password = "root"):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self._get_vm_connection()

    def _get_vm_connection(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(self.hostname, self.port, username=self.username, password=self.password)
        print("Connected to {}".format(self.hostname))

    def _execute_linux(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)

        err = ''.join(stderr.readlines())
        out = ''.join(stdout.readlines())
        final_output = str(out)+str(err)
        return final_output

    def exe(self, cmd):
        return self._execute_linux(cmd)

    def details(self):
        print("Connection Details:")
        print("IP: \t{}".format(self.hostname))
        print("Port: \t{}".format(self.port))
        print("Username: \t{}".format(self.username))
        print("Password: \t{}".format(self.password))

    def transfer_files(self, lpath, rpath):
        ftp_client = self.ssh.open_sftp()
        ftp_client.put(lpath, rpath)
        ftp_client.close()


def transfer_source_files(vm, localfilepath, remotefilepath):
    vm.transfer_files(localfilepath, remotefilepath)


def delete_file_in_dir(vm, dir):
    cmd = (f'rm {dir}')
    out = vm.exe(cmd)
    print(out)


def unzip_source_files(vm):
    cmd = ('cd /home/bdm/data \n'
           'unzip data')
    out = vm.exe(cmd)
    print(out)


def get_files_in_folder(vm, dir):
    cmd = (f'ls {dir}')
    return vm.exe(cmd)


def move_file_to_hdfs(vm, filepath, hdfspath):
    cmd = (f'/home/bdm/BDM_Software/hadoop/bin/hdfs dfs -put {filepath} {hdfspath}')
    print(f"cmd: {cmd}")
    vm.exe(cmd)


def check_if_hdfs_dir_exists(vm, path):
    cmd = (f'/home/bdm/BDM_Software/hadoop/bin/hdfs dfs -test -d /user/{path} \n'
           f'echo $?')
    o = vm.exe(cmd)[0]
    if o == "0":
        return True
    elif o == "1":
        return False


def make_hdfs_dir(vm, dir):
    cmd = (f'/home/bdm/BDM_Software/hadoop/bin/hdfs dfs -mkdir /user/{dir}')
    vm.exe(cmd)


def delete_dir(vm, dir):
    cmd = (f'rmdir {dir}')
    vm.exe(cmd)


def query_composer(q: str, add: str):
    return q + add + "\n"


def move_to_hfds_query(filepath, hdfspath):
    return f'/home/bdm/BDM_Software/hadoop/bin/hdfs dfs -put {filepath} {hdfspath}'


if __name__ == "__main__":
    vm = VM()
    date = datetime.date.today()
    print("_"*100)
    print("Transferring files...")
    print("")
    transfer_source_files(vm, "C:/Users/Pim/Downloads/data.zip", "/home/bdm/data/data.zip")

    print("Unzipping files...")
    print("")
    unzip_source_files(vm)

    print("Deleting zip file...")
    print("")
    delete_file_in_dir(vm, '/home/bdm/data/data.zip')

    files = get_files_in_folder(vm, '/home/bdm/data')
    if not check_if_hdfs_dir_exists(vm, f'temporary'):
        make_hdfs_dir(vm, f'temporary')
    for file in files.split('\n'):
        # The last character of the output of a linux is always "\n" which means that the last element
        # In the created list will be ""
        if file != "":

            print("_" * 100)
            print(f"Checking if /temporary/{file} exists...")
            if not check_if_hdfs_dir_exists(vm, f'temporary/{file}'):
                print(f"/temporary/{file} does not exist. Creating the dir...")
                make_hdfs_dir(vm, f'temporary/{file}')
                print("Success")
                print("")
            else:
                print(f"/temporary/{file} exists")
                print("")

            print("."*100)
            print(f"Checking if /temporary/{file}/{date} exists...")
            if not check_if_hdfs_dir_exists(vm, f'temporary/{file}/{date}'):
                print(f"/temporary/{file}/{date} does not exist. Creating the dir...")
                make_hdfs_dir(vm, f'temporary/{file}/{date}')
                print("")
            else:
                print(f"/temporary/{file}/{date} exists")
                print("")

            print("." * 100)
            print(f"Loading files from {file} to hdfs dir: /user/temporary/{file}/{date}...")

            fs = get_files_in_folder(vm, f'/home/bdm/data/{file}')
            hdfs_query = ""
            delete_query = ""
            for f in fs.split('\n'):

                if f != "":
                    hdfs_query = query_composer(hdfs_query, move_to_hfds_query(f'/home/bdm/data/{file}/{f}', f'/user/temporary/{file}/{date}'))
                    delete_query = query_composer(delete_query, f'rm /home/bdm/data/{file}/{f}')

            vm.exe(hdfs_query)

                     # move_file_to_hdfs(vm, f'/home/bdm/data/{file}/{f}', f'/user/temporary/{file}/{datetime.date.today()}')
                     # delete_file_in_dir(vm, f'/home/bdm/data/{file}/{f}')

            print("All files moved successfully!")
            print("")
            print(f"Deleting /{file}...")
            vm.exe(delete_query)
            delete_dir(vm, f'/home/bdm/data/{file}')
            print("")
            print(f"All files from {file} were successfully processed")
            print("_"*100)
