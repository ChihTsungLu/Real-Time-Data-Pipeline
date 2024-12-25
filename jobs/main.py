import os
import random
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import uuid
import time
LONDON_COORDINATES = {"latitude":51.5074, "longtitude":-0.1278}
BIRMINGHAM_COORDINATES = {"latitude":52.4862, "longtitude":-1.8904}

#  Caculate movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longtitude'] - LONDON_COORDINATES['longtitude']) / 100

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BROKER', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

random.seed(42)

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30,60))
    return start_time


def generate_gps_data(device_id, timestamp, vehicle_type='privat'):
    return{
        'id':uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(10,40),
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }
    
def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'lcoation':location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedImage'
    }

def simulate_vehicle_movement():
    global start_location
    
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longtitude'] += LONGITUDE_INCREMENT
    
    #add some randomness to 
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longtitude'] += random.uniform(-0.0005, 0.0005)
    
    return start_location
    
def generate_weather_data(device_id,timestamp, location):
    return{
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5,26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rainy','Snowing']),
        'precipitation': random.uniform(0,10),
        'windSpeed': random.uniform(0,100),
        'humidity': random.randint(0,100),
        'airQualityIndex': random.uniform(0,500) #AQL Index
    }
        
def generate_vehicle_data(vehicle_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': vehicle_id,
        'timestamp': get_next_time().isoformat(),
        'location':(location['latitude'], location['longtitude']),
        'speed':random.uniform(10,40),
        'direction':'North-East',
        'make':'BMW',
        'model':'X5',
        'year':2024,
        'fuelType':'Hybrid' 
    }
    
def generate_emergency_incident_data(device_id, timestamp, location):
    return{
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'location': location,
        'timestamp': timestamp,
        'type': random.choice(['Fire', 'Accident', 'Robbery', 'Medical', 'Theft']),
        'status': random.choice(['Active', 'Resolved']),
        'description': random.choice(['Situation 1', 'Situation 2', 'Situation 3']),
        'severity': random.choice(['Minor', 'Medium', 'Major', 'Critical']),
    }    
    
def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def producer_data_to_kafa(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    
    producer.flush()
    
def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data =  generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'],'Nikon-1')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        
        if(vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude']
                and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longtitude']):
            print("Vehicle reached Birmingham")
            break
        
        producer_data_to_kafa(producer, VEHICLE_TOPIC, vehicle_data)
        producer_data_to_kafa(producer, GPS_TOPIC, gps_data)
        producer_data_to_kafa(producer, TRAFFIC_TOPIC, traffic_camera_data) 
        producer_data_to_kafa(producer, WEATHER_TOPIC, weather_data)
        producer_data_to_kafa(producer, EMERGENCY_TOPIC, emergency_incident_data)
        
        time.sleep(5)
    
def create_producer():
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}'),
        'security.protocol': 'PLAINTEXT',
        'client.id': 'vehicle-data-producer',
        'retries': 3,
        'retry.backoff.ms': 1000,
        'socket.connection.setup.timeout.ms': 10000,
        'message.timeout.ms': 10000
    }
    
    return SerializingProducer(producer_config)

def test_kafka_connection(producer):
    print(f"Attempting to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    try:
        metadata = producer.list_topics(timeout=10)
        broker_count = len(metadata.brokers)
        print(f"Successfully connected to Kafka cluster with {broker_count} broker(s)")
        print("Available topics:")
        for topic in metadata.topics:
            print(f"  - {topic}")
        return True
    except Exception as e:
        print(f"Failed to connect to Kafka: {str(e)}")
        print("\nTroubleshooting steps:")
        print("1. Check if Docker containers are running:")
        print("   docker-compose ps")
        print("2. Verify Kafka broker logs:")
        print("   docker-compose logs broker")
        print("3. Ensure Kafka broker is healthy:")
        print("   docker-compose exec broker kafka-topics --bootstrap-server localhost:9092 --list")
        return False

if __name__ == '__main__':
    producer = create_producer()
    
    if test_kafka_connection(producer):
        try:
            simulate_journey(producer, 'Vehicle-1')
        except KeyboardInterrupt:
            print("\nGracefully shutting down...")
            producer.flush()  # Ensure any pending messages are sent
        except Exception as e:
            print(f'Unexpected Error occurred: {e}')
    else:
        print("\nConnection test failed. Please check the troubleshooting steps above.")