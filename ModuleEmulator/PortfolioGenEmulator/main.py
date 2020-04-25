#!/usr/bin/env python
import pika
import json
import zipfile
import quandl
import os
import pandas
from zipfile import ZipFile

# Quandl API Key
API_KEY = "b5wYcieS5ZYxGvJNN7yW"


def main():
	# Opens example message for RabbitMQMessage
	with open("PortfolioGeneratorQuery.json", "r") as message:
		message = json.loads(message.read())
		send(message, "backtest", "backtest")
		send(message, "dataFetcher", "dataFetcher")

	recieve()


def callback(ch, method, properties, body):
	try:
		equityCall = body.decode("utf8").replace("\'", "\"")
		equityCall = json.loads(equityCall) 

		print(equityCall)


	except:
		print("RECIEVE: Incorrect RabbitMQ message format")


def recieve():
	connection = pika.BlockingConnection(
	pika.ConnectionParameters(host="localhost"))
	channel = connection.channel()

	channel.queue_declare(queue="backtestResults")
	channel.basic_consume(queue="backtestResults", on_message_callback=callback, auto_ack=True)

	print("RECIEVER: [*] Waiting for messages. To exit press CTRL+C")
	channel.start_consuming()


def send(message, queue, routingKey):
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    channel.queue_declare(queue=queue)

    channel.basic_publish(exchange="", routing_key=routingKey, body=str(message))
    print(" [x] Sent message")

    connection.close()


if __name__ == "__main__":
    main()
