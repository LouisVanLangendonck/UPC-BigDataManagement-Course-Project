import paramiko
import argparse
import datetime


class VM:
    def __init__(self,
                 hostname="10.4.41.59",
                 port=22,
                 username="bdm",
                 password="root"):
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


def get_parser():
    p = argparse.ArgumentParser()
    p.add_argument('-p', '--port', help='Port of the virtual machine', type=int, default=22)
    p.add_argument('-a', '--vmaddress', help='Address of the virtual machine', type=str, default="10.4.41.59")
    p.add_argument('-u', '--username', help='Username of the virtual machine', type=str, default='bdm')
    p.add_argument('-pa', '--password', help='Password of the virtual machine', type=str, default='root')
    return p


def covid_api_to_hdfs(vm):
    date = datetime.date.today()
    url = f'https://api.covid19tracking.narrativa.com/api/{date}/country/spain/region/cataluna/sub_region/barcelona'
    doc_name = f'covid-{date}'
    cmd = f'curl {url} | ~/BDM_Software/hadoop/bin/hdfs dfs -put - /user/bdm/temporary/covid/{date}/{doc_name}'
    vm.exe(cmd)


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    vm = VM(hostname=args.vmaddress, port=args.port, username=args.username, password=args.password)

    covid_api_to_hdfs(vm)
