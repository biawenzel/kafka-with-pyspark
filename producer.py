from kafka import KafkaProducer
from faker import Faker
from datetime import datetime
import time
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def delivery_report(record_metadata):
    print(f"Mensagem entregue no tópico: {record_metadata.topic} - Partição: {record_metadata.partition} - Offset: {record_metadata.offset}")


def send_order(order):
    # Envia a mensagem e adiciona callback para obter informações de entrega
    future = producer.send('vendas', value=order)
    try:
        # Espera a mensagem ser enviada e usa a função delivery_report para exibir informações
        record_metadata = future.get(timeout=10)
        delivery_report(record_metadata)
    except Exception as e:
        print(f"Falha ao enviar mensagem: {e}")

fake = Faker('pt_BR')

def order_generation():
    order_id = fake.random_number(digits=4)
    client_first_name = fake.first_name()
    client_last_name = fake.last_name()
    products = [
        {"product": fake.word(), "quantity": fake.random_int(min=1, max=10), "unit_price": round(fake.random_number(digits=2) + fake.random.random(), 2)}
    ]
    order_total = sum(p['quantity'] * p['unit_price'] for p in products)
    order_date_time = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    order = {
        "order_id": order_id,
        "client_first_name": client_first_name,
        "client_last_name": client_last_name,
        "products": products,
        "order_total": order_total,
        "order_date_time": order_date_time
    }
    return order


for _ in range(10):
    order = order_generation()
    send_order(order)
    time.sleep(1)

print("Todos os pedidos foram enviados com sucesso.")

producer.close()