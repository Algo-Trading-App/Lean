#!/usr/bin/env python
import pika
import json
import zipfile
import quandl
import os
import pandas
from zipfile import ZipFile

# Quandl API Key
API_KEY = "PgJuoJUUrmZVu75mRUD2"


def main():
	# Opens example message for RabbitMQMessage
	with open("message.json", "r") as message:
		message = json.loads(message.read())
		send(message)

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

    channel.queue_declare(queue="backtest")

    channel.basic_consume(queue="backtest", on_message_callback=callback, auto_ack=True)

    print("RECIEVER: [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


def send(message):
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    channel.queue_declare(queue="backtest")

    channel.basic_publish(exchange="", routing_key="backtest", body=str(message))
    print(" [x] Sent message")

    connection.close()

if __name__ == "__main__":
    main()
