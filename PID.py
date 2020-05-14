import docker
import subprocess
import json
from time import sleep

client = docker.from_env()

def query_os():
    stats = []
    containers = client.containers.list() #Gets all containers
    for container in containers:
        stats.append((container.id, container.name)) #Appends containers to a list called stats

    reses = []
    for stat in stats:
        res = subprocess.check_output(["docker", "inspect", "--format", "{{.State.Pid}}", stat[0]]) #Finds the PID
        reses.append(res)

    curState = []
    for x, y in zip(stats, reses):
        curState.append((x[0], x[1], y.decode('utf8').strip()))
    
    return curState

# id, name, pid
if __name__ == "__main__":
    while True:
        response = query_os()
        with open('./orchestrator/PID.file', 'w') as ooFile:
            json.dump(response, ooFile)
        
        with open('./master_slave/PID.file', 'w') as omsFile:
            json.dump(response, omsFile)
        
        sleep(1)
    