#!/usr/bin/env python
import pika
import pymongo
from pymongo import MongoClient
import json
from bson.json_util import dumps
import sys

# Pika setup
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))

channel = connection.channel()


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
    print(" [.] Processing request...on_req_write")
    response = writeData(body)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body=response)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    #send to sync once
    ch.basic_publish(exchange='sync', routing_key='', body=body) #send to sync exchange
    print(" [x] Sent %r" % response)

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

        print(" [.] Accessed records")
        return dumps(results)
        
    else:
        return json.dumps({ "success": False, "message": "Model cannot be blank." })

def on_request_read(ch, method, props, body):
    print(" [.] Processing request:", body)
    response = readData(body)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body=response)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def on_sync(ch, method, properties, body):
    response = writeData(body)

    print(" [x] wrote data on sync -- %r" % response)

mode = sys.argv[1]
if(mode=='master'):
    print(" [x] Master mode")

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
    print(" [x] Slave mode")

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

    print(" [x] Awaiting read_rpc requests")
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



