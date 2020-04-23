import docker
import time

from pymongo import MongoClient

client = docker.from_env()

def spawn_pair(number):
    if(number==0):
        return

    ids = []
    for i in range(number):
        mongo_container = client.containers.run('mongo',
                                            #name='new_mongo',
                                            network='dbaas-network',
                                            detach=True)
        
        mongo_container_id = mongo_container.id

        #TODO: Figure out database copy here

        image = client.images.build(path='/master_slave')
        slave_container = client.containers.run(image[0],
                                        # name='new_master_slave',
                                        volumes={'/Users/richa/Desktop/Sem6/CC/Database-as-a-Service/master_slave': {'bind': '/master_slave'}},
                                        network='dbaas-network',
                                        links={'rmq_host': 'rmq', mongo_container_id:'mongo'},
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
        print(" [o] Spawn watch cycle", cycle)
        res = counts_col.find_one({"name": "default"})
        count = res['count']
        print(" [o] Count is", count)
        to_spawn = count // 20
        new_list = spawn_pair(to_spawn)

        if(new_list):
            print(" [o] Spawned", to_spawn, "contianers with IDs", new_list) 
            for pair in new_list:
                container = containers_col.find_one_and_update({"name": "default"}, {"$push": {"containers": {"mongo": pair[0], "slave": pair[1]}}}, upsert=True)

        set_count = counts_col.find_one_and_update({"name": "default"}, {"$set": {"count": 0}})

        time.sleep(30)

if __name__ == "__main__":
    init_scale_watch()