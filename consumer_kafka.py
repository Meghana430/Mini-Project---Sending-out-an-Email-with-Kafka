from confluent_kafka import Consumer, KafkaError, KafkaException
import smtplib
from email.message import EmailMessage
import json

def create_consumer(config):
    """Create and return a Confluent Kafka consumer."""
    consumer = Consumer(config)
    return consumer

def send_email(message):
    EMAIL_ADDRESS='mskanthadai@ucdavis.edu'
    PASSWORD='*******'
    msg = EmailMessage()
    msg.set_content(message['body'])
    msg['Subject'] = message['subject']
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = message['to_email']

    # Connect to the SMTP server and send the email
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, PASSWORD)
        smtp.send_message(msg)

def consume_messages(consumer, topics):
    """Consume messages from specified Kafka topics."""
    try:
        consumer.subscribe(topics)
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for messages
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message
                print(f"Received message: {msg.value().decode('utf-8')}")
                message=json.loads(msg.value().decode('utf-8'))
                send_email(message)


    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    # Set the address of your Kafka broker and the topic name
    KAFKA_BROKER_ADDRESS = "localhost:29092"
    TOPIC_NAME = "test_topic"

    # Consumer configuration
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER_ADDRESS,
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'client.id': 'confluent_consumer_1'
    }

    # Create a Kafka consumer
    consumer = create_consumer(consumer_config)

    # Start consuming messages
    consume_messages(consumer, [TOPIC_NAME])
