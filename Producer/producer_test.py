from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata

producer = KafkaProducer(bootstrap_servers='localhost:29092', client_id='producer-python')

future: FutureRecordMetadata = producer.send('modification', b'another_message')
future.get(timeout=60)
# Check if the message was sent
if future.is_done:
    print("Message sent")