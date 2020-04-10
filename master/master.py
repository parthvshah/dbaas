#!/usr/bin/env python
import pika
import pymongo
from pymongo import MongoClient
import json
from bson.json_util import dumps

# Mongo setup
client = MongoClient('mongodb://localhost:27017')
db = client.ride_share_db_dev
Ride = db.rides
User = db.users
Model = db.users

# Pika setup
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')

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

def on_request(ch, method, props, body):
    print(" [.] Processing request")
    response = writeData(body)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body=response)
    ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__=="__main__":
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

    print(" [x] Awaiting requests")
    channel.start_consuming()