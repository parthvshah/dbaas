import json
from bson.json_util import dumps
import sys
from kazoo.client import KazooClient, KazooState
import socket
import logging
import subprocess
import os
from time import sleep

# Zookeeper setup
zk = KazooClient(hosts='zoo')
zk.start()
myid = str(socket.gethostname())

def id_helper(myid):
    pid_arr = []
    with open("PID.file",) as msFile:
        pid_arr = json.load(msFile)
        for container in pid_arr:
            for field in container:
                if(myid in field):
                    return container[2]

# For switch
print(" [~] Setup watch.")
while True:
    if(zk.exists("/election/master")):
        data, stat = zk.get("election/master")
        if(data):
            master_pid = str(data.decode('utf-8'))
            print(" [~] Master exists;", master_pid)

            if((id_helper(myid) == master_pid) and (zk.exists("/slave/"+str(id_helper(myid))))):
                print(" [~] Switching to master.")
                os.system("pkill -f master_slave.py")
                print(" [~] Killed.")
                os.system("python -u master_slave.py")

    # When master dies, waiting for election
    else:
        print(" [~] Node does not exist.")


    sleep(10)
