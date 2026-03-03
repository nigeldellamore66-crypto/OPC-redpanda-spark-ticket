import json
import random
import time
import os
from datetime import datetime, timezone
from kafka import KafkaProducer
import uuid

# Configuration du producer
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8") # convertit l'objet python en json, puis l'objet json en bytes ( pour kafka)
)

TOPIC = os.getenv("TOPIC", "client-tickets")

# Données simulées
types_demande = ["incident", "facturation", "technique", "information"]
priorites = ["basse", "moyenne", "haute", "critique"]

descriptions = [
    "Problème de connexion au service",
    "Erreur sur la facture",
    "Application qui plante",
    "Demande d'information produit",
    "Impossible d'accéder au compte",
    "Problème de paiement",
    "Bug interface utilisateur"
]


def generate_ticket():
    return {
        "ticket_id": str(uuid.uuid4()),
        "client_id": random.randint(1000, 9999),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "demande": random.choice(descriptions),
        "type_demande": random.choice(types_demande),
        "priorite": random.choice(priorites)
    }


if __name__ == "__main__":
    print("Production de tickets en cours...")

    try:
        while True:
            ticket = generate_ticket()
            producer.send(TOPIC, ticket)
            print(f"Ticket envoyé : {ticket}")
            time.sleep(2) 

    except KeyboardInterrupt: # CTRL + C
        print("Arrêt du producer.")
        producer.close()