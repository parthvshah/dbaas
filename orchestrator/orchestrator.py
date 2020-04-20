from flask import Flask
from flask import request,jsonify
from send import RpcClient #send_to_writeQ,send_to_readQ,     # defined in this dir
import pika
import json
import sys
app = Flask(__name__)

gReadCount = 0

#!/usr/bin/env python
@app.route('/')   #demo function
def hello():
    return "Hello World! How are you"
@app.route('/api/v1/db/write',methods = ['POST'])
def send_to_master():
    if not request.json:
        abort(400)
    print (request.is_json)
    content = request.get_json()
    print (content)
    #send contents fo rabbitmq(pika)
    print(" [x] Requesting to master")
    db_rpc = RpcClient("writeQ");

    print(" [x] Requesting to master")
    response = db_rpc.call(content)
    response=json.loads(response)
    #obtain results
    print(" [.] Got %r" % response)
    return jsonify(response),201  #send it back to user/rides microservice #jsonify

@app.route('/api/v1/db/read', methods = ['POST'])
def send_to_slaves():
    print (request.is_json)
    content = request.get_json()
    print (content)
    # send to readQ(contents)
    db_rpc = RpcClient("readQ");

    print(" [x] Requesting to slave")
    response = db_rpc.call(content) #call sends it into the q
    #obtain results
    print(" [.] Got %r" % response)

    gReadCount += 1

    return ("response_json",response),201 #send it back to user/rides microservice #jsonify

@app.route('/api/v1/orch/readcount', methods=['GET'])
def get_read_count():
    print("Count:", gReadCount)
    return jsonify([gReadCount]),200

if __name__ == '__main__':
    #app.run()
    app.run(debug=True, host='0.0.0.0')