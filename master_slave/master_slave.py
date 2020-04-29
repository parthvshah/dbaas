#!/usr/bin/env python
import uuid
import pika
import pymongo
from pymongo import MongoClient
import json
from bson.json_util import dumps
import sys
from kazoo.client import KazooClient, KazooState
import socket
import logging
import subprocess
import os

logging.basicConfig()

# Pika setup
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))

channel = connection.channel()

# Zookeeper setup
zk = KazooClient(hosts='zoo')
zk.start()
myid = str(socket.gethostname())

def my_listener(state):
    if state == KazooState.LOST:
        # Register somewhere that the session was lost
        print(" [ms] Connection error - lost")

    elif state == KazooState.SUSPENDED:
        # Handle being disconnected from Zookeeper
        print(" [ms] Connection error - suspended")

    else:
        # Handle being connected/reconnected to Zookeeper
        print(" [ms] Connected to zoo")

zk.add_listener(my_listener)

def id_helper(myid):
    pid_arr = []
    with open("PID.file",) as msFile:
        pid_arr = json.load(msFile)
        for container in pid_arr:
            for field in container:
                if(myid in field):
                    return container[2]

def writeData(req):
    req = json.loads(req)
    try:
        model = req['model']
    except KeyError:
        model = ''
    try:
        parameters = req['parameters']
    except KeyError:
        parameters = {}
    try:
        operation = req['operation']
    except KeyError:
        operation = ''
    try:
        query = req['query']
    except KeyError:
        query = {}

    if(model and operation):
        if(not parameters):
            return json.dumps({ "success": False, "message": "Parameters cannot be blank." })

        if (operation == "update" and not query):
            return json.dumps({ "success": False, "message": "Query cannot be blank." })

        if (model):
            if (model == "Ride"):
                Model = Ride
            if(model == "User"):
                Model = User
        
        # Insert
        if(operation == "insert"):
            try:
                insertedID = Model.insert_one(parameters).inserted_id
            except:
                return json.dumps({ "success": False, "message": "Insert one error." })
        
        # Delete
        if(operation == "delete"):
            try:
                Model.find_one_and_delete(parameters)
            except:
                return json.dumps({ "success": False, "message": "Find one and delete error." })
        
        # Update
        if(operation == "update"):
            try:
                Model.find_one_and_update(query, parameters)
            except:
                return json.dumps({ "success": False, "message": "Find one and update error." })
                
        return json.dumps({ "success": True, "message": "DB write done" })

    else:
        return json.dumps({ "success": False, "message": "Error: Model and operation cannot be blank." })

def on_request_write(ch, method, props, body):
    print(" [m] Processing request...on_req_write")
    response = writeData(body)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body=response)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    #send to sync once
    ch.basic_publish(exchange='sync', routing_key='', body=body) #send to sync exchange
    print(" [m] Sent %r" % response)

def readData(req):
    req = json.loads(req)
    try:
        model = req['model']
    except KeyError:
        model = ''
    try:
        parameters = req['parameters']
    except KeyError:
        parameters = {}

    if (model):
        if (model == "Ride"):
            Model = Ride
        if(model == "User"):
            Model = User
        
        try:
            results = Model.find(parameters)
        except:
            return json.dumps({ "success": False, "message": "Find error." })

        print(" [s] Accessed records")
        return dumps(results)
        
    else:
        return json.dumps({ "success": False, "message": "Model cannot be blank." })

def on_request_read(ch, method, props, body):
    print(" [s] Processing request:", body)
    response = readData(body)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body=response)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def on_sync(ch, method, properties, body):
    response = writeData(body)

    print(" [m] Wrote data on sync -- %r" % response)

def master_mode(re_election=False):
    print(" [m] Master mode")
   
    zk.create("/master/"+str(id_helper(myid)), b"master", ephemeral=True, makepath=True)
    print(" [m] Zookeper master node created")
    
    client = MongoClient('mongo_1')
    db = client.dbaas_db
    Ride = db.rides
    User = db.users
    Model = db.users

    channel.queue_declare(queue='write_rpc')
    channel.exchange_declare(exchange='sync', exchange_type='fanout')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='write_rpc', on_message_callback=on_request_write)
    print(" [x] Awaiting requests write_rpc_requests")
    if(re_election):
        pass
    else:
        channel.start_consuming()

def slave_mode():
    print(" [s] Slave mode")
    
    zk.create("/slave/"+str(id_helper(myid)), b"slave", ephemeral=True, makepath=True)
    print(" [s] Zookeper slave node created") 

    client = MongoClient('mongo_2')
    db = client.dbaas_db
    Ride = db.rides
    User = db.users
    Model = db.users

    channel.queue_declare(queue='read_rpc')
    channel.exchange_declare(exchange='sync', exchange_type='fanout')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='read_rpc', on_message_callback=on_request_read)
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue #random queue name generated... temporary q
    channel.queue_bind(exchange='sync', queue=queue_name)
    channel.basic_consume(
        queue=queue_name, on_message_callback=on_sync, auto_ack=True)

    print(" [s] Awaiting read_rpc requests")
    channel.start_consuming()

# For switch
@zk.DataWatch("/election/master")
def watch_node(data, stat):
    try:
        master_watch_pid = data.decode("utf-8")
    except:
        master_watch_pid = -1

    if(id_helper(myid) == master_watch_pid):
        print(" [ms] I am master.")
        if(zk.exists("/slave/"+str(id_helper(myid)))):
            print(" [ms] Switching to master.")
            # os.execl(sys.executable, 'python', __file__, *sys.argv[1:])
            exit()
        

# For init
if(zk.exists('/election/master')):
    slave_mode()
else:
    master_pid = str(id_helper(myid))
    zk.create("/election/master", master_pid.encode('utf-8'), ephemeral=True, makepath=True)
    master_mode()