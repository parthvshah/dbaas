import pika
import uuid
import json
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
    print("Sent to readQ")
    connection.close()
def receive_from_responseQ():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    print(' [*] Waiting for messages from responseQ. To exit press CTRL+C')


    def set_response_to_global_var(ch, method, properties, body):
        print(" [x] Received from responseQ%r" % body)
        # time.sleep(body.count(b'.'))
        print(" [x] Done")
        global response
        response=body
        ch.basic_ack(delivery_tag=method.delivery_tag)


    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='responseQ', on_message_callback=set_response_to_global_var)

    channel.start_consuming()




class RpcClient(object):

    def __init__(self,q_name):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq'))
        self.queue_name=q_name

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue=q_name)#, exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(                  #subscribe to callback q to receive 
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, content):
        self.response = None
        self.corr_id = str(uuid.uuid4()) #unique id
        print("publishing to",self.queue_name)
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(content))
        while self.response is None:
            self.connection.process_data_events()
        return (self.response)


# db_rpc = RpcClient()

# print(" [x] Requesting fib(30)")
# response = db_rpc.call(30)
# print(" [.] Got %r" % response)