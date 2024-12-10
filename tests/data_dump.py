import random
from time import sleep
from faker import Faker
import requests
from shapely.geometry import Point, shape
import json

# Load GeoJSON land polygons
def load_land_polygons(file_path="land_polygons.geojson"):
    with open(file_path, "r") as f:
        return json.load(f)

# Check if a point is on land
def is_on_land(lat, lon, polygons):
    point = Point(lon, lat)
    for feature in polygons["features"]:
        polygon = shape(feature["geometry"])
        if polygon.contains(point):
            return True
    return False

# Generate a random latitude and longitude that falls on land
def generate_land_lat_lon(polygons):
    while True:
        latitude = random.uniform(-90, 90)
        longitude = random.uniform(-180, 180)
        if is_on_land(latitude, longitude, polygons):
            return round(latitude, 6), round(longitude, 6)

# Initialize Faker
fake = Faker()

# For validation
view_id = set()
click_id = set()

# Function to generate random data
def generate_random_data(polygons):
    # Define actions with probabilities
    ad_id = str(random.randint(1, 10))
    action = 'view'
    if ad_id in view_id:
        action = random.choices(['view', 'click'], [0.8, 0.2])[0]
    if action == 'click' and ad_id in click_id:
        action = random.choices(["click", "conversion"], [0.8, 0.2])[0]
    if action == 'view':
        view_id.add(ad_id)
    if action == 'click':
        click_id.add(ad_id)

    # Generate land-based latitude and longitude
    latitude, longitude = generate_land_lat_lon(polygons)

    return {
        "ad_id": ad_id,
        "campaign_id": str(random.randint(1, 100)),
        "creative_id": str(random.randint(1, 500)),
        "action": action,
        "user_id": fake.user_name(),
        "session_id": fake.uuid4(),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
        "operating_system": random.choice(["Android", "iOS", "Windows", "macOS"]),
        "ip_address": fake.ipv4(),
        "ad_placement": random.choice(["homepage", "sidebar", "footer"]),
        "referrer_url": fake.url(),
        "destination_url": fake.url(),
        "cpc": float(round(random.uniform(0.01, 2.00), 2)),  # Ensure cpc is a float
        "latitude": latitude,
        "longitude": longitude,
    }

# Function to send data to the server
def send_data_to_server(url, polygons):
    while True:
        data = generate_random_data(polygons)
        try:
            response = requests.post(url, json=data)
            if response.status_code == 200:
                print(f"Successfully sent: {data}")
            else:
                print(f"Failed to send data: {response.status_code}, {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error sending data: {e}")
        
        # Generate a random sleep interval between 0 and 5 seconds
        interval = random.uniform(0, 5)
        print(f"Sleeping for {interval:.2f} seconds...")
        sleep(interval)

if __name__ == "__main__":
    # Load land polygons once at startup
    land_polygons = load_land_polygons("land_polygons.geojson")

    server_url = "http://localhost:5000/events"
    print(f"Sending random data to {server_url} with land-based locations and random intervals...")
    try:
        send_data_to_server(server_url, land_polygons)
    except KeyboardInterrupt:
        print("\nData generation stopped.")