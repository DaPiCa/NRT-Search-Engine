from confluent_kafka import Producer

longitud = 1

p = Producer({'bootstrap.servers': 'localhost:29092'})
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]\n\tLongitud({})'.format(msg.topic(), msg.partition(), longitud))

while True:
    cadena = 'a' * longitud
    p.produce('topic1', cadena.encode('utf-8'), callback=delivery_report)
    longitud += 1

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()