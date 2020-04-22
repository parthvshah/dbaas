from flask import Flask, request, jsonify
from flask_script import Manager, Server
from send import RpcClient #send_to_writeQ,send_to_readQ,     # defined in this dir
import pika
import json
import sys

import docker
import time

from pymongo import MongoClient

myclient = MongoClient('orch_mongo')
# db = myclient["dbaas"]




client = docker.from_env()

def spawn_pair(number):
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
                                        volumes={'/home/parth/Documents/College/CC/Project/Database-as-a-Service/master_slave': {'bind': '/master_slave'}},
                                        network='database-as-a-service_default',
                                        links={'rmq_host': 'rmq', mongo_container_id:'mongo'},
                                        restart_policy={"Name": "on-failure", "MaximumRetryCount": 5},
                                        command='sh -c "sleep 15 && python -u master_slave.py slave"',
                                        detach=True)
        ids.append((mongo_container_id, slave_container.id))
    
    return ids


def init_scale_watch():


    cycle = 0

    while True:
        cycle += 1
        print(" [x] Spawn watch cycle", cycle)

        # Add db call here to get cur count
        # assuming collection is called counts
        count_col = myclient.dbaas.counts
        # check if it returns a key value pair
        count = count_col.find()
        # assuming key name is count
        inc = { "$inc": {"count": 1}}
        count_col.update(count, inc)

        # count = 25

        to_spawn = count // 20
        new_list = spawn_pair(to_spawn)
        print(" [x] Spawned", to_spawn, "contianers with IDs", new_list)   

        # Add db call here to set new list of contianers (append)
        container_col =  myclient.db.containers

        setz = {"$set:" {"count": 0}}
        time.sleep(120)
        count.col.update(count, setz)

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
    print(" [x] Requesting to master")
    db_rpc = RpcClient("writeQ");

    print(" [x] Requesting to master")
    response = db_rpc.call(content)
    response=json.loads(response)
    #obtain results
    print(" [.] Got %r" % response)
    return jsonify(response),201  #send it back to user/rides microservice #jsonify

@app.route('/api/v1/db/read', methods = ['POST'])
def send_to_slaves():
    print (request.is_json)
    content = request.get_json()
    print (content)
    # send to readQ(contents)
    db_rpc = RpcClient("readQ");

    print(" [x] Requesting to slave")
    response = db_rpc.call(content) #call sends it into the q
    #obtain results
    print(" [.] Got %r" % response)

    # add db call to update count
    return ("response_json",response),201 #send it back to user/rides microservice #jsonify

if __name__ == '__main__':
    # app.run(debug=True, host='0.0.0.0')
    manager.run()