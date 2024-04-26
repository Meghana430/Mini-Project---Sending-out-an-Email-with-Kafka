from confluent_kafka import Producer
import json

def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {str(msg)}: {str(err)}")
    else:
        print(f"Message produced: {msg.topic()} {str(msg.partition())} {msg.offset()}")

def create_producer(config):
    """Create and return a Confluent Kafka producer."""
    return Producer(config)

def send_messages(producer, topic):
    """Send messages to the Kafka topic."""
    try:
        message = {'id': 1,'subject':'Hey cutie pie' ,'body': "Hello madeee this email from Kafka codeee",'to_email':'mskanthadai@ucdavis.edu'}
        # Convert message to string and encode to bytes
        producer.produce(topic, json.dumps(message).encode('utf-8'), callback=acked)
        producer.poll(0)
        producer.flush()  # Wait until all messages are delivered
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Producer closed.")

if __name__ == "__main__":
    # Set the address of your Kafka broker and the topic name
    KAFKA_BROKER_ADDRESS = "localhost:29092"
    TOPIC_NAME = "test_topic"

    # Producer configuration
    producer_config = {
        'bootstrap.servers': KAFKA_BROKER_ADDRESS,
        'client.id': 'confluent_producer_1'
    }

    # Create a Kafka producer
    producer = create_producer(producer_config)

    # Start sending messages
    send_messages(producer, TOPIC_NAME)
