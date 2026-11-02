
from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id="building_admin"
)

my_name = "student"

topics = [
    f"building_sensors_{my_name}",
    f"temperature_alerts_{my_name}",
    f"humidity_alerts_{my_name}"
]

existing_topics = admin_client.list_topics()

new_topics = [
    NewTopic(
        name=topic,
        num_partitions=1,
        replication_factor=1
    )
    for topic in topics
    if topic not in existing_topics
]

if new_topics:
    admin_client.create_topics(new_topics=new_topics)

print("Created topics:")

for topic in topics:
    print(topic)

admin_client.close()
