import docker
import time
import requests

client = docker.from_env()

containers_spawned = 0
#(mongo, slave)
containers_spawned_ids = []

running_containers = 1

def spawn_pair(number):
    ids = []
    for i in range(number):
        mongo_container = client.containers.run('mongo',
                                            #name='new_mongo',
                                            network='database-as-a-service_default',
                                            detach=True)
        
        mongo_container_id = mongo_container.id

        #TODO: Figure out database copy here

        image = client.images.build(path='./master_slave')
        slave_container = client.containers.run(image[0],
                                        # name='new_master_slave',
                                        volumes={'/home/parth/Documents/College/CC/Project/Database-as-a-Service/master_slave': {'bind': '/slave'}},
                                        network='database-as-a-service_default',
                                        links={'d5220b32036d': 'rmq', mongo_container_id:'mongo'},
                                        working_dir='/slave',
                                        restart_policy={"Name": "on-failure", "MaximumRetryCount": 5},
                                        command='sh -c "sleep 15 && python -u master_slave.py slave"',
                                        detach=True)
        ids.append((mongo_container_id, slave_container.id))
    
    return ids


if __name__ == "__main__":
    while True:
        time.sleep(2*60)

        res = requests.get('http://localhost:5000/api/v1/orch/readcount')
        count = res.json()[0]

        to_spawn = count // 20
        new_list = spawn_pair(to_spawn)
        containers_spawned_ids.extend(new_list)
        print(" [x] Spawned", to_spawn, "contianers with IDs", new_list)   
