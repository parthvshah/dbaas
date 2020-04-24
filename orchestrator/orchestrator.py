from flask import Flask, request, jsonify
from flask_script import Manager, Server
import pika
import json
import sys

import uuid
import threading
from time import sleep
from multiprocessing.pool import ThreadPool

pool = ThreadPool(processes=2)

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

class ReadRpcClient(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)
    
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body
    
    def call(self, request):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='read_rpc',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=request)
        while self.response is None:
            self.connection.process_data_events()
        print(" [rh] Response", self.response)
        return self.response
class WriteRpcClient(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)
    
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body
    
    def call(self, request):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='write_rpc',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=request)
        while self.response is None:
            self.connection.process_data_events()
        print(" [rh] Response", self.response)
        return self.response

@app.route('/')   #demo function
def hello():
    return "Hello World! How are you?"

@app.route('/api/v1/db/write',methods = ['POST'])
def send_to_master():
    content = request.get_json()
    # print(content)

    write_rpc = WriteRpcClient()
    async_res = pool.apply_async(write_rpc.call, (json.dumps(content),))
    response = async_res.get().decode('utf8')    
    if(not response):
        return '', 204 
    else:
        return response, 200
    # if not request.json:
    #     abort(400)
    # print (request.is_json)
    # content = request.get_json()
    # print (content)
    # #send contents fo rabbitmq(pika)
    # print(" [o] Requesting to master")
    # db_rpc = RpcClient("writeQ")

    # print(" [o] Requesting to master")
    # response = db_rpc.call(content)
    # response=json.loads(response)
    # #obtain results
    # print(" [o] Got %r" % response)
    # return json.dumps(response),201  #send it back to user/rides microservice #jsonify

@app.route('/api/v1/db/read', methods = ['POST'])
def send_to_slaves():
    content = request.get_json()
    # print(content)

    read_rpc = ReadRpcClient()
    async_res = pool.apply_async(read_rpc.call, (json.dumps(content),))
    response = async_res.get().decode('utf8')

    count = counts_col.find_one_and_update({"name": "default"}, {"$inc": {"count": 1}})
    if(not response):
        return '', 204 
    else:
        return response, 200

if __name__ == '__main__':
    # app.run(debug=True, host='0.0.0.0')
    manager.run()