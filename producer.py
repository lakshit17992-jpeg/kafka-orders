from flask import Flask, request, jsonify
from confluent_kafka import Producer
import json

app = Flask(__name__)

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

@app.route('/order', methods=['POST'])
def create_order():
    data = request.get_json()
    order = {
        'order_id': data['order_id'],
        'item': data['item'],
        'quantity': data['quantity']
    }
    producer.produce(
        'orders',
        key=str(order['order_id']),
        value=json.dumps(order),
        callback=delivery_report
    )
    producer.flush()
    return jsonify({'status': 'order published', 'order': order}), 201


if __name__ == '__main__':
    app.run(debug=True, port=5000)