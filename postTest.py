import requests
import json
import random

url = 'http://localhost:1026/v2/entities'

headers = {
    'Content-Type': 'application/json',
}
for i in range(1, 21):
    data = {
    "id": f"Kitchen{i}",
    "type": "Room",
    "temperature": {
        "value": random.uniform(5, 40),
        "type": "Float"
    },
    "pressure": {
        "value": random.randint(600, 700),
        "type": "Integer"
    }
    }
    print(f"Sending data: {json.dumps(data)}")

    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code == 201:
        print("Éxito!", response.status_code, response.text)
        
    else:
        print(response.status_code)
        print(response.text)