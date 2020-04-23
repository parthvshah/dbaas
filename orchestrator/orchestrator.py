from flask import Flask, request, jsonify
from flask_script import Manager, Server
from send import RpcClient ,send_to_writeQ,send_to_readQ,receive_from_responseQ     # defined in this dir
import pika
import json
import sys

import docker
import time

client = docker.from_env()

def spawn_pair(number):
    ids = []
    for i in range(number):
        mongo_container = client.containers.run('mongo',
                                            #name='new_mongo',
                                            network='database_as_service_default',
                                            detach=True)
        
        mongo_container_id = mongo_container.id

        #TODO: Figure out database copy here

        image = client.images.build(path='/master_slave')
        slave_container = client.containers.run(image[0],
                                        # name='new_master_slave',
                                        volumes={'/home/rajath/Documents/DataBase_as_Service': {'bind': '/master_slave'}},
                                        network='database_as_service_default',
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
        count = 25

        to_spawn = count // 20
        new_list = spawn_pair(to_spawn)
        print(" [x] Spawned", to_spawn, "contianers with IDs", new_list)   

        # Add db call here to set new list of contianers (append)

        time.sleep(120)
        count = 0

class CustomServer(Server):
    def __call__(self, app, *args, **kwargs):
        init_scale_watch()
        return Server.__call__(self, app, *args, **kwargs)

app = Flask(__name__)
manager = Manager(app)
manager.add_command('runserver', )
manager.add_command("runserver", CustomServer(
    use_debugger = True,
    use_reloader = True,
    host = '0.0.0.0') )

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
    # db_rpc = RpcClient("writeQ");

    # print(" [x] Requesting to master")
    # response = db_rpc.call(content)
    send_to_writeQ(content)
    print("[x] sent to writeQ")
    # response=None #set this value inside receive_from_responseQ
    # response=receive_from_responseQ()
    # response=json.loads(response) #convert to json
    # if response is not None:
    #     print(" [.] Got %r" % response)
    #     return jsonify(response),201  #send it back to user/rides microservice #jsonify
    # print("[x] No response received from responseQ)

@app.route('/api/v1/db/read', methods = ['POST'])
def send_to_slaves():
    print (request.is_json)
    content = request.get_json()
    print (content)
    # send to readQ(contents)
    # db_rpc = RpcClient("readQ");
    # print(" [x] Requesting to slave")
    # response = db_rpc.call(content) #call sends it into the q
    # #obtain results
    # print(" [.] Got %r" % response)
    send_to_readQ(content)
    response=None #set this value inside receive_from_responseQ
    response=receive_from_responseQ()
    response=json.loads(response) #convert to json      # note responseQ as db data
    if response is not None:
        print(" [.] Got %r" % response)
        return jsonify(response),201  #send it back to user/rides microservice #jsonify
    else :
        print("[x] No response received from responseQ")
    # add db call to update count
    #send it back to user/rides microservice #jsonify

if __name__ == '__main__':
    # app.run(debug=True, host='0.0.0.0')
    manager.run(host='0.0.0.0')