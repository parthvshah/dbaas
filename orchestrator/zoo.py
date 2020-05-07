from kazoo.client import KazooClient, KazooState
from time import sleep
import json
import uuid

# Zookeper setup
zk = KazooClient(hosts='zoo')
zk.start()

def id_helper(myid):
    pid_arr = []
    with open("PID.file",) as zFile:
        pid_arr = json.load(zFile)
        for container in pid_arr:
            for field in container:
                if(myid in field):
                    return container[2]

def conduct_election():
    print(" [z] Conducting election.")

    children = zk.get_children("/slave")

    int_children = []
    for child in children:
        int_children.append(int(child))

    sorted_int_children = sorted(int_children)
    dec_pid = str(sorted_int_children[0])

    zk.create("/election/master", dec_pid.encode('utf-8'), ephemeral=True, makepath=True)
    # TODO: Spawn a slave


if __name__ == "__main__":
    retry_count = 0
    retry_limit = 10
    while True:
        try:
            data, stat = zk.get("/election/master")
            print(" [z] Master is", data.decode("utf-8"))
            retry_count = 0
            retry_limit = 2
        except:
            retry_count += 1
            print(" [z] Retrying. Count", retry_count)

            if(retry_count==retry_limit):
                conduct_election()
                retry_count = 0

        sleep(10)