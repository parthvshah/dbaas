from flask import Flask, request, jsonify
from flask_script import Manager, Server
from send import RpcClient #send_to_writeQ,send_to_readQ,     # defined in this dir
import pika
import json
import sys

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
                                            network='database-as-a-service_default',
                                            detach=True)
        
        mongo_container_id = mongo_container.id

        #TODO: Figure out database copy here

        image = client.images.build(path='/master_slave')
        slave_container = client.containers.run(image[0],
                                        # name='new_master_slave',
                                        volumes={'/Users/richa/Desktop/Sem6/CC/Database-as-a-Service/master_slave': {'bind': '/master_slave'}},
                                        network='database-as-a-service_default',
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

    set_count = counts_col.find_one_and_update({"name": "default"}, {"$set": {"count": 25}}, upsert=True)

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

class CustomServer(Server):
    def __call__(self, app, *args, **kwargs):
        init_scale_watch()
        return Server.__call__(self, app, *args, **kwargs)

app = Flask(__name__)
manager = Manager(app)
manager.add_command('runserver', CustomServer())

#!/usr/bin/env python
@app.route('/')   #demo function
def hello():
    return "Hello World! How are you?"

@app.route('/api/v1/db/write',methods = ['POST'])
def send_to_master():
    if not request.json:
        abort(400)
    print (request.is_json)
    content = request.get_json()
    print (content)
    #send contents fo rabbitmq(pika)
    print(" [o] Requesting to master")
    db_rpc = RpcClient("writeQ");

    print(" [o] Requesting to master")
    response = db_rpc.call(content)
    response=json.loads(response)
    #obtain results
    print(" [o] Got %r" % response)
    return jsonify(response),201  #send it back to user/rides microservice #jsonify

@app.route('/api/v1/db/read', methods = ['POST'])
def send_to_slaves():
    print (request.is_json)
    content = request.get_json()
    print (content)
    # send to readQ(contents)
    db_rpc = RpcClient("readQ");

    print(" [o] Requesting to slave")
    response = db_rpc.call(content) #call sends it into the q
    #obtain results
    print(" [o] Got %r" % response)

    count = counts_col.find_one_and_update({"name": "default"}, {"$inc": {"count": 1}})
    return ("response_json",response),201 #send it back to user/rides microservice #jsonify

if __name__ == '__main__':
    # app.run(debug=True, host='0.0.0.0')
    manager.run()