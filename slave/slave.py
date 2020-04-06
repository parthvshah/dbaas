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
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')

def readData(req):
    req = json.loads(req)
    model = req['model']
    parameters = req['parameters']

    if (model):
        if (model == "Ride"):
            Model = Ride
        if(model == "User"):
            Model = User
        
        try:
            results = Model.find(parameters)
        except:
            return json.dumps({ "success": False, "message": "Find error." })

        return dumps(results)
        

    else:
        return json.dumps({ "success": False, "message": "Model cannot be blank." })

def on_request(ch, method, props, body):
    print(" [.] Processing request")
    response = readData(body)

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