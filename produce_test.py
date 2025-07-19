from kafka import KafkaProducer
import json
import random
from datetime import datetime, timezone
import time

# Funzione per creare una misura casuale
def create_random_measure():
    measure_units = ["Celsius"]
    return {
        "value": round(random.uniform(20, 30), 2),
        "measureUnit": random.choice(measure_units),
        "time": datetime.now(timezone.utc).isoformat(),
        "nodeId": 1 #random.randint(1, 10)
    }

#producer = KafkaProducer(bootstrap_servers='100.78.181.75:9092')  # IP Tailscale del broker

# Kafka producer configurato per inviare JSON
producer = KafkaProducer(
    bootstrap_servers='100.78.181.75:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Parto a mandare messaggi casuali")
# Invia 10 messaggi di esempio
while(True):
    for _ in range(10):
        measure = create_random_measure()
        producer.send('measures', value=measure)
        #print(f"Messaggio inviato: {measure}")
        time.sleep(1)  # Attendi 1 secondo tra un invio e l'altro
    time.sleep(3)

print("Stop")
#producer.send('test', b'Ciao dal Pi Zero!')
producer.flush()
print("Messaggio inviato!")
