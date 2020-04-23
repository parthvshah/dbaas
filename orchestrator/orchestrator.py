from flask import Flask, request, jsonify
from flask_script import Manager, Server
from send import RpcClient #send_to_writeQ,send_to_readQ,     # defined in this dir
import pika
import json
import sys

from pymongo import MongoClient

myclient = MongoClient("orch_mongo")    
db = myclient['orch']
counts_col = db['counts']
containers_col = db['containers']

class CustomServer(Server):
    def __call__(self, app, *args, **kwargs):
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
    db_rpc = RpcClient("writeQ")

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
    db_rpc = RpcClient("readQ")

    print(" [o] Requesting to slave")
    response = db_rpc.call(content) #call sends it into the q
    #obtain results
    print(" [o] Got %r" % response)

    count = counts_col.find_one_and_update({"name": "default"}, {"$inc": {"count": 1}})
    return ("response_json",response),201 #send it back to user/rides microservice #jsonify

if __name__ == '__main__':
    # app.run(debug=True, host='0.0.0.0')
    manager.run()