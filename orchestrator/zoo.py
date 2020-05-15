from kazoo.client import KazooClient, KazooState
from time import sleep
import json
import uuid
import docker
import os

from scale_watch import spawn_pair_export

# Zookeper setup
zk = KazooClient(hosts='zoo')
zk.start()

# Docker setup
client = docker.from_env()
PATH = os.getenv('HOSTPWD')

master_init_complete = False
slave_init_complete = False
slave_count = 0

def id_helper(myid):
    pid_arr = []
    with open("PID.file",) as zFile:
        pid_arr = json.load(zFile)
        for container in pid_arr:
            for field in container:
                if(myid in field):
                    return container[2]

# Checks for master and slave every ten seconds
def conduct_election():
    print(" [z] Conducting election.")

    children = zk.get_children("/slave")

    int_children = []
    for child in children:
        int_children.append(int(child))

    sorted_int_children = sorted(int_children)
    dec_pid = str(sorted_int_children[0])

    sleep(10)
    # zk.create("/election/master", dec_pid.encode('utf-8'), ephemeral=True, makepath=True)
    new_list = spawn_pair_export(1, PATH)
    print(" [z] Replaced master with ms containers with IDs", new_list)

def replace_ms():
    print(" [z] Replacing.")
    new_list = spawn_pair_export(1, PATH)
    print(" [z] Replaced slave with ms containers with IDs", new_list)


if __name__ == "__main__":
    retry_count = 0
    retry_limit = 10
    s_retry_count = 0
    s_retry_limit = 10
    s_prev = 0
    while True:
        # Retry count gives time for the worker containers to spawn before election
        # For master
        try:
            data, stat = zk.get("/election/master")
            # PID print
            print(" [z] Master is", data.decode("utf-8"))
            retry_count = 0
            retry_limit = 2
        except:
            retry_count += 1
            print(" [z] Master not available. Retry count", retry_count)

            if(retry_count==retry_limit):
                conduct_election()
                retry_count = 0
        
        # For slave
        try:
            children = zk.get_children("/slave")
            print(" [z] Slaves are", children)
            s_retry_count = 0
            s_retry_limit = 2

            if(len(children) < s_prev):
                replace_ms()
            else:
                s_prev = len(children)

            
        except:
            s_retry_count += 1
            print(" [z] Slave(s) not available. Retry count", s_retry_count)

            # if(s_retry_count==s_retry_limit):
            #     replace_ms()
            #     s_retry_count = 0

        sleep(10)
