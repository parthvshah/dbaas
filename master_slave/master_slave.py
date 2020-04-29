#!/usr/bin/env python
import uuid
import pika
import pymongo
from pymongo import MongoClient
import json
from bson.json_util import dumps
import sys
from kazoo.client import KazooClient, KazooState
import logging
logging.basicConfig()

# Pika setup
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))

channel = connection.channel()

# Zookeeper setup
zk = KazooClient(hosts='zoo')
zk.start()

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

mode = sys.argv[1]
if(mode=='master'):
    print(" [m] Master mode")

    pid_arr = []
    with open("PID.file",) as mFile:
        pid_arr = json.load(mFile)
        for container in pid_arr:
            if('master' in container):
                zk.create("/master/"+str(container[2]), b"Surprise!", ephemeral=True, makepath=True)
                print(" [m] Zookeper node created")
                break

    
    client = MongoClient('master_mongo')
    db = client.dbaas_db
    Ride = db.rides
    User = db.users
    Model = db.users

    channel.queue_declare(queue='write_rpc')
    channel.exchange_declare(exchange='sync', exchange_type='fanout')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='write_rpc', on_message_callback=on_request_write)
    print(" [x] Awaiting requests write_rpc_requests")
    channel.start_consuming()

if(mode=='slave'):
    print(" [s] Slave mode")

    pid_arr = []
    with open("PID.file",) as sFile:
        pid_arr = json.load(sFile)
        for container in pid_arr:
            if('slave' in container):
                zk.create("/slave/"+str(container[2]), b"Surprise!", ephemeral=True, makepath=True)
                print(" [s] Zookeper node created")
                break

    client = MongoClient('slave_mongo')
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

    # channel.queue_declare(queue='readQ')

    # channel.basic_qos(prefetch_count=1)
    # channel.basic_consume(queue='readQ', on_message_callback=on_request_read)

    # channel.exchange_declare(exchange='sync', exchange_type='fanout')

    # result = channel.queue_declare(queue='', exclusive=True)
    # queue_name = result.method.queue #random queue name generated... temporary q

    # channel.queue_bind(exchange='sync', queue=queue_name)

    # print(' [*] waiting for  messages.')



    # channel.basic_consume(
    #     queue=queue_name, on_message_callback=on_sync, auto_ack=True)


    # print(" [x] Awaiting requests")
    # channel.start_consuming()



