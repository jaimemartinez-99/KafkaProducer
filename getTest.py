import time
import json
import requests
from pykafka import KafkaClient

KAFKA_HOST = "localhost:9092"  # Or the address you want
client = KafkaClient(hosts=KAFKA_HOST)
topic = client.topics["testing"]

url = 'http://localhost:1026/v2/entities'
headers = {
    'Accept': 'application/json'
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    # Successful response
    json_data = response.json()
    print(json_data)

    with topic.get_sync_producer() as producer:
        # Send the JSON data to Kafka
        message = json.dumps(json_data)
        print(message)
        encoded_message = message.encode("utf-8")
        print(encoded_message)
        producer.produce(encoded_message)
        print("Mensaje enviado")
else:
    # Handle the error
    print(f"Error: {response.status_code} - {response.text}")
