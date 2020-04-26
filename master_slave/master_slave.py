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

    # ch.basic_publish(exchange='',
    #                  routing_key=props.reply_to,
    #                  properties=pika.BasicProperties(correlation_id = props.correlation_id),
    #                  body=response)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    #send to sync once
    ch.basic_publish(exchange='sync', routing_key='', body=body) #send to sync exchange
    print(" [x] Sent to sync--%r" % body)

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
    print(" [.] Processing request")
    response = readData(body)
    ch.queue_declare(queue='responseQ', durable=True)

    # ch.basic_publish(exchange='',
    #                  routing_key=props.reply_to,
    #                  properties=pika.BasicProperties(correlation_id = props.correlation_id),
    #                  body=response)
    ch.basic_publish(exchange='',
                        routing_key='responseQ',
                        body=json.dumps(response),
                        properties=pika.BasicProperties(
                            delivery_mode = 2, # make message persistent
                        ))
    print("Sent to responseQ")
    ch.basic_ack(delivery_tag=method.delivery_tag)
def on_sync(ch, method, properties, body):
    response = writeData(body)

    print(" [x] wrote data on sync -- %r" % response)

mode = sys.argv[1]
if(mode=='master'):
    print(" [x] Master mode")

    client = MongoClient('master_mongo')
    db = client.ride_share_db_dev
    Ride = db.rides
    User = db.users
    Model = db.users

    channel.queue_declare(queue='writeQ',durable=True)
    channel.exchange_declare(exchange='sync', exchange_type='fanout')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='writeQ', on_message_callback=on_request_write)
    print(" [x] Awaiting requests to write from orchestrator")
    channel.start_consuming()

if(mode=='slave'):
    print(" [x] Slave mode")

    client = MongoClient('slave_mongo')
    db = client.ride_share_db_dev
    Ride = db.rides
    User = db.users
    Model = db.users
    #note slaves have to cater to readQ.. sync with master .. and push data to responseq
    channel.queue_declare(queue='readQ',durable=True)
    channel.exchange_declare(exchange='sync', exchange_type='fanout')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='readQ', on_message_callback=on_request_read)
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue #random queue name generated... temporary q
    channel.queue_bind(exchange='sync', queue=queue_name)
    channel.basic_consume(
        queue=queue_name, on_message_callback=on_sync, auto_ack=True)
    print(" [x] Awaiting requests to read from orchestrator")
    channel.start_consuming()
    
    
#Zookeeper code starts from here.

from kazoo.client import KazooClient
from kazoo.client import KazooState
import time

def my_listener(state):		#just listening to the state of connection
	if state == KazooState.LOST:
        	print("Connection Lost/Closed")
	elif state == KazooState.SUSPENDED:
        	print("Connection Suspended")
	else:
        	print("Welcome to the Zookeeper")

def with_zeros(node,length):	# while converting the node ids to integar, the leading zeros disappears. They are being added back here
	zeros_node=str(node).zfill(length)
	return zeros_node

def previous(children,mynode,length):	# Find the next lowest node to the current node
	new_children=pop_leader_node(children)
	children1=map(int, new_children)	# Converting values to integar as I was getting an error while using max()
	if int(mynode)==min(children1):
		print("You are the smallest '%s' so become the leader now!!" %mynode)
		p_node=mynode
	else:
		my_node=max(t for t in children1 if t<int(mynode))
		print("Current node id is %s " %mynode)
		p_node=with_zeros(my_node,length)
		print("Previous node id is %s" %p_node)
	return p_node

def smallest(length):	# To find the smallest node 
	children=zk.get_children("/leader")
	children_int= map(int, children)
#	print("children in smallest %s" %children_int)
	node=min(int(s) for s in children_int)
	s_node=with_zeros(node,length)	
#	print("Smallest node id is %s" %s_node)
	return s_node

def pop_leader_node(children):	#to discard the leader_node temporarily for for processing
	if zk.exists("/leader/leader_node"):
		pop_i=children.index('leader_node')
		del children[pop_i]
	else:
		pass
	return children

def check(event):	#the is the function that will be called when the watch is triggered
	leader(mynode, children, length)

def leader(mynode, children, length):	
	p_node=previous(children,mynode,length)	# Let's see what the next smallest node is	
	if zk.exists("/leader/leader_node"):    # Check to see if the leader node exists
		data,stat=zk.get("/leader/leader_node") #extract the information about current leader
		print("Current Leader is %s" %data)
	else:   # If no leader node is present
		s_node=smallest(length)        # Find smallest node in the list
		if int(mynode)==int(s_node):    # If our node is the smallest
			print("Hey you are the leader!! %s" %mynode)
			zk.create("/leader/leader_node", mynode.encode('utf8'), ephemeral= True)        #as our node is the leader so become one
		else:	# Our node is not the smallest so just move towards the watch part
			pass
	zk.exists("/leader/%s"%p_node, watch=check)

zk = KazooClient(hosts='zoo:2181')
zk.add_listener(my_listener)
zk.start()

if zk.exists("/leader"):
	first=0		# assigning this number just to find out if this is first time node is entering the leader election or not
	print("leader node is present")

	
	while True:
		
		if first==0:
			mynode=zk.create("/leader/", "", sequence= True, ephemeral= True)[8:]	#Creates a node for itself
			length=len(mynode)
			children=zk.get_children("/leader")
			leader(mynode, children, length)
			first=first+1
		else:
			children=zk.get_children("/leader")
			length=len(mynode)
			p_node=previous(children,mynode,length)
			print(p_node)			
			
			print(" Watching the next smaller node")

		#time.sleep(5)

else:
	print("No leader election")
	
