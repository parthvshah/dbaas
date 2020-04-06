from flask import Flask
from flask import request,jsonify
import pika
import sys
app = Flask(__name__)

#!/usr/bin/env python
@app.route('/')
def hello():
    return "Hello World!"
def send_to_writeQ(contents):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
    channel = connection.channel()

    channel.queue_declare(queue='writeQ', durable=True)
    channel.basic_publish(exchange='',
                        routing_key='writeQ',
                        body=json.dumps(contents),
                        properties=pika.BasicProperties(
                            delivery_mode = 2, # make message persistent
                        ))
    print("Sent to writeQ")
    connection.close()
def send_to_readQ(contents):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
    channel = connection.channel()

    channel.queue_declare(queue='readQ', durable=True)
    channel.basic_publish(exchange='',
                        routing_key='readQ',
                        body=json.dumps(contents),
                        properties=pika.BasicProperties(
                            delivery_mode = 2, # make message persistent
                        ))
    print(" Sent to readQ" %)
    connection.close()



@app.route('/api/v1/db/write',methods = ['POST'])
def send_to_master():
    if not request.json:
        abort(400)
    print (request.is_json)
    content = request.get_json()
    print (content)
    send_to_writeQ(contents)
    #send contents fo rabbitmq(pika)
    #obtain results
    #return json obj
    sample_json=[
    {
        'id':1,
        'name': "hello"
    }
    ]
    return jsonify("sample_json",sample_json),201

@app.route('/api/v1/db/read', methods = ['POST'])
def send_to_slaves():
    print (request.is_json)
    content = request.get_json()
    print (content)
    send_to_readQ(contents)
    #send contents fo rabbitmq(pika)
    #obtain results
    #return json obj
    return jsonify("sample_json",sample_json)
if __name__ == '__main__':
    app.run()
    # app.run(debug=True, host='0.0.0.0')