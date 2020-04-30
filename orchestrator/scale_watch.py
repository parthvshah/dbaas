import docker
from time import sleep
from pymongo import MongoClient
import json

from kazoo.client import KazooClient, KazooState


client = docker.from_env()
PATH = '/home/parth/Documents/College/CC/Project/Database-as-a-Service'
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

def spawn_pair(number):
    ids = []
    for i in range(number):
        mongo_container = client.containers.run('mongo',
                                            #name='new_mongo',
                                            volumes={PATH+'/orchestrator': {'bind': '/data'}},
                                            network='dbaas-network',
                                            restart_policy={"Name": "on-failure", "MaximumRetryCount": 5},
                                            detach=True)
        
        mongo_container_id = mongo_container.id
        sleep(5)
        output = mongo_container.exec_run('bash -c "cd /data && mongorestore --archive="db-dump" --nsFrom="dbaas_db.*" --nsTo="dbaas_db.*""')

        image = client.images.build(path='/master_slave')
        slave_container = client.containers.run(image[0],
                                        # name='new_master_slave',
                                        volumes={PATH+'/master_slave': {'bind': '/master_slave'}},
                                        network='dbaas-network',
                                        environment=['MONGO_ID='+mongo_container_id],
                                        links={'rmq_host': 'rmq', mongo_container_id: 'mongo'},
                                        restart_policy={"Name": "on-failure", "MaximumRetryCount": 5},
                                        command='sh -c "sleep 30 && chmod a+x run.sh && ./run.sh"',
                                        detach=True)
        ids.append((mongo_container_id, slave_container.id))
    global newly_spawned_pairs
    newly_spawned_pairs += number
    return ids

def down_pair(number):
    ids = []
    for i in range(number):
        ids.append(spawned_record.pop())

    for pair in ids:
        mongo_container = client.containers.get(pair[0])
        slave_container = client.containers.get(pair[1])

        mongo_container.stop()
        slave_container.stop()
    
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

        if(cycle==1):
            new_list = spawn_pair(2)
            spawned_record.extend(new_list)
            print(" [sw] Init spawn contianers with IDs", new_list) 
            for pair in new_list:
                container = containers_col.find_one_and_update({"name": "default"}, {"$push": {"containers": {"mongo": pair[0], "slave": pair[1]}}}, upsert=True)

        res = counts_col.find_one({"name": "default"})
        count = res['count']
        print(" [sw] Count is", count)
        to_spawn = count // 20

        delta = 2 + to_spawn - newly_spawned_pairs
        if(delta>0):
            new_list = spawn_pair(abs(delta))
            spawned_record.extend(new_list)
            print(" [sw] Spawned", delta, "contianers with IDs", new_list) 
            for pair in new_list:
                container = containers_col.find_one_and_update({"name": "default"}, {"$push": {"containers": {"mongo": pair[0], "slave": pair[1]}}}, upsert=True)
        
        if(delta<0):
            down_list = down_pair(abs(delta))
            print(" [sw] Downed", delta, "contianers with IDs", down_list) 
            for pair in down_list:
                container = containers_col.find_one_and_update({"name": "default"}, {"$pull": {"containers": {"mongo": pair[0], "slave": pair[1]}}})


        set_count = counts_col.find_one_and_update({"name": "default"}, {"$set": {"count": 0}})

        sleep(2*60)

@zk.ChildrenWatch("/master")
def watch_master(children):
    print(" [sw] Master is: %s" % children)

@zk.ChildrenWatch("/slave")
def watch_slaves(children):
    print(" [sw] Slave(s) are: %s" % children)

if __name__ == "__main__":
    init_scale_watch()