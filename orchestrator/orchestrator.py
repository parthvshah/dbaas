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

from kazoo.client import KazooClient, KazooState

# Zookeper setup
zk = KazooClient(hosts='zoo')
zk.start()

import docker
client = docker.from_env()

from zoo import replace_ms

def pid_helper(myid):
    pid_arr = []
    with open("PID.file",) as oFile:
        pid_arr = json.load(oFile)
        for container in pid_arr:
            for field in container:
                if(str(myid) == str(field)):
                    # returns name of container given pid
                    return container[1]

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
        # print(" [rh] Response", self.response)
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
        # print(" [rh] Response", self.response)
        return self.response

@app.route('/')   #demo function
def hello():
    return "Hello World! How are you?"

@app.route('/api/v1/db/write',methods = ['POST'])
def send_to_master():
    content = request.get_json()
    print(" [debug] Write request:", content)

    write_rpc = WriteRpcClient()
    async_res = pool.apply_async(write_rpc.call, (json.dumps(content),))
    response = async_res.get().decode('utf8')

    master_pid = ""
    if(zk.exists("/master")):
        data, stat = zk.get("election/master")
        if(data):
            master_mongo_name = str(data.decode('utf-8'))
    
    print(" [o] Master mongo name:", master_mongo_name)
    if(len(master_mongo_name)!=0):
        master_mongo = client.containers.get(master_mongo_name)
        output = master_mongo.exec_run('bash -c "mongodump --archive="/data/db-dump" --db=dbaas_db"')
        print(" [o] Dumped DB.", output)
        sleep(1)

    print(" [debug] Write response:", response)
    if(not response):
        return '', 204 
    else:
        return response, 200

@app.route('/api/v1/db/read', methods = ['POST'])
def send_to_slaves():
    content = request.get_json()
    print(" [debug] Read request:", content)

    read_rpc = ReadRpcClient()
    async_res = pool.apply_async(read_rpc.call, (json.dumps(content),))
    response = async_res.get().decode('utf8')

    count = counts_col.find_one_and_update({"name": "default"}, {"$inc": {"count": 1}})
    sleep(1)

    print(" [debug] Read response:", response)
    if(not response):
        return '', 204 
    else:
        return response, 200

@app.route('/api/v1/worker/list', methods = ['GET'])
def worker_list():
    children = zk.get_children("/slave")
    pids = []
    for child in children:
        pids.append(int(child))

    masters = zk.get_children("/master")
    for master in masters:
        pids.append(int(master))
    
    response = sorted(pids)
    print(" [o] PIDs returned.")

    return json.dumps(response), 200

@app.route('/api/v1/crash/master', methods = ['POST'])
def crash_master():
    data, stat = zk.get("/election/master")
    master_name = pid_helper(data.decode('utf-8'))
    master_container = client.containers.get(master_name)
    master_container.stop()

    print(" [o] Master Stopped;", str(master_name))
    response = []
    return json.dumps(response), 200

@app.route('/api/v1/crash/slave', methods = ['POST'])
def crash_slave():
    children = zk.get_children("/slave")
    pids = []
    for child in children:
        pids.append(int(child))

    sorted_pids = sorted(pids)
    stop_pid = sorted_pids[0]
    stop_ms_name = pid_helper(stop_pid)
    print(" [o] Sorted PIDs:", sorted_pids)

    stop_ms_container = client.containers.get(stop_ms_name)
    stop_ms_container.stop()
    print(" [o] Stopped ms named:", str(stop_ms_name))

    data, stat = zk.get("/slave/"+str(stop_pid))
    stop_mongo_name = pid_helper(data.decode('utf-8'))

    stop_mongo_container = client.containers.get(stop_mongo_name)
    stop_mongo_container.stop()
    print(" [o] Stopped mongo named:", str(stop_mongo_name))

    replace_ms()
    response = []
    return json.dumps(response), 200

@app.route('/api/v1/db/clear', methods = ['POST'])
def clear_db():
    content = request.get_json()
    clear_rpc = WriteRpcClient()
    async_res = pool.apply_async(clear_rpc.call, (json.dumps(content),))
    response = async_res.get().decode('utf8')
    return response, 200

if __name__ == '__main__':
    # app.run(debug=True, host='0.0.0.0')
    manager.run()