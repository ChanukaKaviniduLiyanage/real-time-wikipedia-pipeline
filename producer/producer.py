# producer/producer.py (Final Version)
import json
import sys
from kafka import KafkaProducer
import sseclient
import requests

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

if __name__ == "__main__":
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    
    # Add a User-Agent to identify our script
    headers = {
        'Accept': 'text/event-stream',
        'User-Agent': 'wikipulse-project/0.1 (A data engineering learning project)'
    }

    try:
        print("Attempting to connect to Wikipedia stream with a User-Agent...")
        response = requests.get(url, stream=True, headers=headers)
        
        if response.status_code != 200:
            print(f"Error: Received non-200 status code: {response.status_code}")
            print(f"Response content: {response.text}")
            sys.exit(1)

        client = sseclient.SSEClient(response)
        print("Connection successful! Starting to process and produce messages...")
        
        message_count = 0
        for event in client.events():
            if event.event == 'message':
                try:
                    message_count += 1
                    change = json.loads(event.data)
                    producer.send('wikipedia-edits', change)
                    if message_count % 10 == 0:
                      print(f"Produced edit #{message_count} for: {change['meta']['uri']}")
                    
                except (json.JSONDecodeError, KeyError):
                    pass
        
        print("Stream ended.")

    except requests.exceptions.RequestException as e:
        print(f"A network error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        print("Closing Kafka producer.")
        producer.close()