import pika
import json

def main():
	# Opens example message for RabbitMQMessage
	with open("dataFetcherQuery.json", "r") as message:
		message = json.loads(message.read())
		send(message)

def send(message):
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    channel.queue_declare(queue="dataFetcher")

    channel.basic_publish(exchange="", routing_key="dataFetcher", body=str(message))
    print(" [x] Sent message")

    connection.close()

if __name__ == "__main__":
    main()
