from confluent_kafka import Consumer, KafkaError
import json

# Configure the consumer with broker address and group settings
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-processors',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false'
})

# Subscribe to the orders topic
consumer.subscribe(['orders'])

print("Consumer started. Waiting for orders...")
try:
    while True:
        # Poll for a message with a 1-second timeout
        msg = consumer.poll(1.0)

        # No message available yet
        if msg is None:
            continue

        # Handle any errors from the broker
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        # Deserialize and process the order
        order = json.loads(msg.value().decode('utf-8'))
        print(f"Processing order: {order}")
        print(f"  Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")

        # Manually commit the offset after successful processing
        consumer.commit()
        print(f"  Offset committed.")

except KeyboardInterrupt:
    print("Shutting down consumer...")
finally:
    # Always close the consumer to leave the group cleanly
    consumer.close()