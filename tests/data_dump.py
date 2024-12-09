import random
from time import sleep
from faker import Faker
import requests

# Initialize Faker
fake = Faker()

# Function to generate random data
def generate_random_data():
    # Define actions with probabilities
    actions = ["view", "click", "conversion"]
    action_weights = [0.8, 0.15, 0.05]  # View is most frequent, followed by click, then conversion

    return {
        "ad_id": '1',  # Fixed for testing, can be randomized
        "campaign_id": str(random.randint(1, 100)),
        "creative_id": str(random.randint(1, 500)),
        "action": random.choices(actions, weights=action_weights, k=1)[0],  # Weighted choice
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
        "latitude": float(round(fake.latitude(), 4)),  # Ensure latitude is a float
        "longitude": float(round(fake.longitude(), 4)),  # Ensure longitude is a float
    }

# Function to send data to the server
def send_data_to_server(url):
    while True:
        data = generate_random_data()
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
    server_url = "http://localhost:5000/events"
    print(f"Sending random data to {server_url} with random intervals...")
    try:
        send_data_to_server(server_url)
    except KeyboardInterrupt:
        print("\nData generation stopped.")