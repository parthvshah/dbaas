import docker
from time import sleep
from pymongo import MongoClient
import json
from random import randint
import os

from kazoo.client import KazooClient, KazooState

client = docker.from_env()
HOSTPWD = os.getenv('HOSTPWD')
print(" [sw] PATH:", HOSTPWD)
spawned_record = []
newly_spawned_pairs = 0

# Zookeper setup
zk = KazooClient(hosts='zoo')
zk.start()

def get_stats():
    data = None
    with open('./PID.file') as iFile:
        try:
            data = json.load(iFile)
        except:
            pass
    
    return data

def spawn_pair_export(number, PATH):
    ids = []
    for i in range(number):
        generated_mongo_name = 'new_mongo_'+str(randint(0,999))
        mongo_container = client.containers.run('mongo',
                                            name=generated_mongo_name,
                                            volumes={PATH+'/orchestrator': {'bind': '/data'}},
                                            network='dbaas-network',
                                            entrypoint='mongod --bind_ip_all',
                                            restart_policy={"Name": "on-failure", "MaximumRetryCount": 5},
                                            detach=True)
        
        mongo_container_id = mongo_container.id
        mongo_container_name = mongo_container.name

        sleep(3)
        #Copies the db to the new container. Silently fails if there is no db to restore
        output = mongo_container.exec_run('bash -c "cd /data && mongorestore --archive="db-dump" --nsFrom="dbaas_db.*" --nsTo="dbaas_db.*""')
        #name generator
        generated_ms_name = 'new_ms_'+str(randint(0,999))
        image = client.images.build(path='/master_slave')
        slave_container = client.containers.run(image[0],
                                        name=generated_ms_name,
                                        volumes={PATH+'/master_slave': {'bind': '/master_slave'}},
                                        network='dbaas-network',
                                        environment=['MONGO_NAME='+mongo_container_name],
                                        links={'rmq_host': 'rmq', mongo_container_name: 'mongo'},
                                        restart_policy={"Name": "on-failure", "MaximumRetryCount": 5},
                                        command='sh -c "sleep 3 && chmod a+x run.sh && ./run.sh"',
                                        detach=True)
        ids.append((mongo_container_id, slave_container.id))

    return ids

#Spawn container
def spawn_pair(number):
    ids = []
    for i in range(number):
        generated_mongo_name = 'new_mongo_'+str(randint(0,999))
        mongo_container = client.containers.run('mongo',
                                            name=generated_mongo_name,
                                            volumes={HOSTPWD+'/orchestrator': {'bind': '/data'}},
                                            network='dbaas-network',
                                            entrypoint='mongod --bind_ip_all',
                                            restart_policy={"Name": "on-failure", "MaximumRetryCount": 5},
                                            detach=True)
        
        mongo_container_id = mongo_container.id
        mongo_container_name = mongo_container.name

        sleep(5)
        output = mongo_container.exec_run('bash -c "cd /data && mongorestore --archive="db-dump" --nsFrom="dbaas_db.*" --nsTo="dbaas_db.*""')

        generated_ms_name = 'new_ms_'+str(randint(0,999))
        image = client.images.build(path='/master_slave')
        slave_container = client.containers.run(image[0],
                                        name=generated_ms_name,
                                        volumes={HOSTPWD+'/master_slave': {'bind': '/master_slave'}},
                                        network='dbaas-network',
                                        environment=['MONGO_NAME='+mongo_container_name],
                                        links={'rmq_host': 'rmq', mongo_container_name: 'mongo'},
                                        restart_policy={"Name": "on-failure", "MaximumRetryCount": 5},
                                        command='sh -c "sleep 5 && chmod a+x run.sh && ./run.sh"',
                                        detach=True)
        ids.append((mongo_container_id, slave_container.id))

    global newly_spawned_pairs
    newly_spawned_pairs += number
    return ids

# Take down container
def down_pair(number):
    ids = []
    for i in range(number):
        ids.append(spawned_record.pop())

    for pair in ids:
        mongo_container = client.containers.get(pair[0])
        slave_container = client.containers.get(pair[1])

        mongo_container.stop()
        slave_container.stop()

    global newly_spawned_pairs
    newly_spawned_pairs -= number
    return ids

def init_scale_watch():
    myclient = MongoClient("orch_mongo")    
    db = myclient['orch']
    counts_col = db['counts']
    containers_col = db['containers']
    cycle = 0

    set_count = counts_col.find_one_and_update({"name": "default"}, {"$set": {"count": 0}}, upsert=True)

    while True:
        cycle += 1
        print(" [sw] Spawn watch cycle", cycle)

        # For init
        if(cycle==1):
            new_list = spawn_pair(2)
            spawned_record.extend(new_list)
            print(" [sw] Init spawn containers with IDs", new_list) 
            sleep(30)
        
        res = counts_col.find_one({"name": "default"})
        count = res['count']
        print(" [sw] Count is", count)
        to_spawn = count // 20

        print(" [sw] to_spawn:", to_spawn, "newly_spawned_pairs:", newly_spawned_pairs)
        delta = 2 + to_spawn - newly_spawned_pairs #two containers are spawned in first cycle

        if(delta>0): #Scale up a pair
            new_list = spawn_pair(abs(delta))
            spawned_record.extend(new_list)
            print(" [sw] Spawned", delta, "containers with IDs", new_list) 
        
        if(delta<0): #scale down a pair
            down_list = down_pair(abs(delta))
            print(" [sw] Downed", delta, "containers with IDs", down_list) 

        set_count = counts_col.find_one_and_update({"name": "default"}, {"$set": {"count": 0}})

        sleep(2*60)

# @zk.ChildrenWatch("/master")
# def watch_master(children):
#     print(" [sw] Master is: %s" % children)

# @zk.ChildrenWatch("/slave")
# def watch_slaves(children):
#     print(" [sw] Slave(s) are: %s" % children)

if __name__ == "__main__":
    init_scale_watch()
