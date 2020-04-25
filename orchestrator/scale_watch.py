import docker
from time import sleep
from pymongo import MongoClient
import json

client = docker.from_env()
PATH = '/home/parth/Documents/College/CC/Project/Database-as-a-Service'

def get_stats():
    data = None
    with open('./PID.file') as iFile:
        try:
            data = json.load(iFile)
        except:
            pass
    
    return data

def spawn_pair(number):
    if(number==0):
        return

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
                                        links={'rmq_host': 'rmq', mongo_container_id: 'mongo'},
                                        restart_policy={"Name": "on-failure", "MaximumRetryCount": 5},
                                        command='sh -c "sleep 15 && python -u master_slave.py slave"',
                                        detach=True)
        ids.append((mongo_container_id, slave_container.id))
    
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
        res = counts_col.find_one({"name": "default"})
        count = res['count']
        print(" [sw] Count is", count)
        to_spawn = count // 20
        new_list = spawn_pair(to_spawn)

        if(new_list):
            print(" [sw] Spawned", to_spawn, "contianers with IDs", new_list) 
            for pair in new_list:
                container = containers_col.find_one_and_update({"name": "default"}, {"$push": {"containers": {"mongo": pair[0], "slave": pair[1]}}}, upsert=True)

        set_count = counts_col.find_one_and_update({"name": "default"}, {"$set": {"count": 0}})

        sleep(2*60)

if __name__ == "__main__":
    init_scale_watch()