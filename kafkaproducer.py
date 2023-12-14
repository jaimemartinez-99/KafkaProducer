from pykafka import KafkaClient
import threading
import csv
import json
import time

KAFKA_HOST = "localhost:9092" # Or the address you want

client = KafkaClient(hosts = KAFKA_HOST)
topic = client.topics["datos_IOT"]

with open('datos.csv', encoding='utf-8-sig') as file:
    reader = csv.DictReader(file)
    headers = next(reader) # ['Station Name', 'Measurement Timestamp', 'Air Temperature', 'Wet Bulb Temperature', 'Humidity', 'Rain Intensity', 'Interval Rain', 'Total Rain', 'Precipitation Type', 'Wind Direction', 'Wind Speed', 'Maximum Wind Speed', 'Barometric Pressure', 'Solar Radiation', 'Heading', 'Battery Life', 'Measurement Timestamp Label', 'Measurement ID']
    #print(headers)
    with topic.get_sync_producer() as producer:
        for row in reader:
            json_row = json.dumps(row, indent=2)
            print(json_row)
            message = json_row
            encoded_message = message.encode("utf-8")
            producer.produce(encoded_message)
            # Mandar cada 2 segundos. A razon de 5 mensajes por batch del Apache Spark
            time.sleep(2)
    
        





        
    
    
    

    