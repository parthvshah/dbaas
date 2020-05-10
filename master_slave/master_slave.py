#!/usr/bin/env python
import uuid
import pika
import pymongo
from pymongo import MongoClient
import json
import sys
from kazoo.client import KazooClient, KazooState
import socket
import logging
import subprocess
import os
from time import sleep

logging.basicConfig()

# Pika setup
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))

channel = connection.channel()

# Zookeeper setup
zk = KazooClient(hosts='zoo')
zk.start()
myid = str(socket.gethostname())

# Mongo setup
MONGO_NAME = os.getenv('MONGO_NAME')
print(" [ms] Environ:", MONGO_NAME)

client = MongoClient(MONGO_NAME)
db = client.dbaas_db
Ride = db.rides
User = db.users
Model = db.users

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
    except:
        model = ''
    try:
        parameters = req['parameters']
    except:
        parameters = {}
    try:
        operation = req['operation']
    except:
        operation = 'clear'
    try:
        query = req['query']
    except:
        query = {}

    if(operation == "clear"):
        try:
            client.drop_database(db)

            return json.dumps({ "success": True, "message": "DB write done" })

        except:
            return json.dumps({ "success": False, "message": "Failed to clear total db." })

    if(model and operation):
        # if(not parameters):
        #     return json.dumps({ "success": False, "message": "Parameters cannot be blank." })

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
    print(" [m] Sent to sync: %r" % body)

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

        return_arr = []
        for result in results:
            result['_id'] = str(result['_id'])
            return_arr.append(result)

        print(" [s] Accessed records;", json.dumps(return_arr))
        return json.dumps(return_arr)
        
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
    print(" [s] Sent %r" % response)

def on_sync(ch, method, properties, body):
    response = writeData(body)

    print(" [s] Wrote data on sync: %r" % response)

def master_mode():
    print(" [m] Master mode. Mongo name:", MONGO_NAME)
   
    zk.create("/master/"+str(id_helper(myid)), MONGO_NAME.encode('utf-8'), ephemeral=True, makepath=True)
    print(" [m] Zookeper master node created")

    channel.queue_declare(queue='write_rpc')
    channel.exchange_declare(exchange='sync', exchange_type='fanout')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='write_rpc', on_message_callback=on_request_write)
    print(" [m] Awaiting requests write_rpc_requests")
    channel.start_consuming()

def slave_mode():
    print(" [s] Slave mode. Mongo name:", MONGO_NAME)
    
    zk.create("/slave/"+str(id_helper(myid)), MONGO_NAME.encode('utf-8'), ephemeral=True, makepath=True)
    print(" [s] Zookeper slave node created")

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

# For init
if(zk.exists('/election/master')):
    slave_mode()
else:
    master_pid = str(id_helper(myid))
    zk.create("/election/master", master_pid.encode('utf-8'), ephemeral=True, makepath=True)
    master_mode()