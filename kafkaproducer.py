from pykafka import KafkaClient
import threading
import csv
import json
import time
from datetime import datetime


KAFKA_HOST = "localhost:9092" # Or the address you want

client = KafkaClient(hosts = KAFKA_HOST)
topic = client.topics["datos_IOT"]

def convert_data_types(row):
    row["Air_Temperature"] = float(row["Air_Temperature"]) if row["Air_Temperature"] else None
    row["Wet_Bulb_Temperature"] = float(row["Wet_Bulb_Temperature"]) if row["Wet_Bulb_Temperature"] else None
    row["Humidity"] = float(row["Humidity"]) if row["Humidity"] else None
    row["Rain_Intensity"] = float(row["Rain_Intensity"]) if row["Rain_Intensity"] else None
    row["Interval_Rain"] = float(row["Interval_Rain"]) if row["Interval_Rain"] else None
    row["Total_Rain"] = float(row["Total_Rain"]) if row["Total_Rain"] else None
    row["Wind_Direction"] = float(row["Wind_Direction"]) if row["Wind_Direction"] else None
    row["Wind_Speed"] = float(row["Wind_Speed"]) if row["Wind_Speed"] else None
    row["Maximum_Wind_Speed"] = float(row["Maximum_Wind_Speed"]) if row["Maximum_Wind_Speed"] else None
    row["Barometric_Pressure"] = float(row["Barometric_Pressure"]) if row["Barometric_Pressure"] else None
    row["Solar_Radiation"] = float(row["Solar_Radiation"]) if row["Solar_Radiation"] else None
    row["Heading"] = float(row["Heading"]) if row["Heading"] else None
    row["Battery_Life"] = float(row["Battery_Life"]) if row["Battery_Life"] else None
    return row

with open('datos.csv', encoding='utf-8-sig') as file:
    reader = csv.DictReader(file)
    headers = next(reader) 
    with topic.get_sync_producer() as producer:
        for row in reader:
            row = convert_data_types(row)
            json_row = json.dumps(row, indent=2)
            print(json_row)
            message = json_row
            encoded_message = message.encode("utf-8")
            producer.produce(encoded_message)
            # Mandar cada 2 segundos. A razon de 5 mensajes por batch del Apache Spark.
            time.sleep(2)
    
        





        
    
    
    

    