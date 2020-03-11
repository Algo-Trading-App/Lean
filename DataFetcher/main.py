#!/usr/bin/env python
import pika
import json
import zipfile
import quandl
import os
import pandas
from zipfile import ZipFile
import operator


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
		securityCall = body.decode("utf8").replace("\'", "\"")
		sercurityCall = json.loads(securityCall)

		# Gets each equity for each timeframe
		for timeFrame in securityCall["timeFrames"]:

			#haewon code: if the security is equity or forex or option or future or crpyto
			if "equities" in timeFrame:
				for ticker in timeFrame["equities"]:
					writeData(timeFrame, ticker)
			elif "forex" in timeFrame:
				for ticker in timeFrame["forex"]:
					writeData(timeFrame, ticker)

			elif "options" in timeFrame:
				for ticker in timeFrame["options"]:
					writeData(timeFrame, ticker)
			
			elif "futures" in timeFrame:
				for ticker in timeFrame["futures"]:
					writeData(timeFrame, ticker)


			elif "crpyto" in timeFrame:
				for ticker in timeFrame["crpyto"]:
					writeData(timeFrame, ticker)


	except:
		print("RECIEVE: Incorrect RabbitMQ message format")



def getData(equityCall, ticker):
	
	# Makes Quandl API call for ticker as provided by RabbitMQMessage
	df = quandl.get(
                "WIKI/"+ticker,
                start_date=equityCall["startTime"],
                end_date=equityCall["endTime"],
                api_key=API_KEY)

        # Multiply values by 10000 to fit Lean format
        for header in df.columns[0:4].tolist():
                df[header] = df[header].apply(lambda x: int(x * 10000))

        df["Volume"] = df["Volume"].apply(lambda x: int(x))
        df.index = pandas.to_datetime(df.index,
                format = '%m/%d/%Y').strftime('%Y%m%d 00:00')

        # Drop unused columns from dataframe
        df = df.drop(["Ex-Dividend",
                        "Split Ratio",
                        "Adj. Open",
                        "Adj. High",
                        "Adj. Low",
                        "Adj. Close",
                        "Adj. Volume"],
                        axis=1)

	return df

def writeData(securityCall, ticker):
	# If path for equity does not exist create one

	if "equities" in securityCall:
		outname = ticker.lower() + ".csv"
		zipname = ticker.lower() + ".zip"

		outdir = "../Data/equity/usa/"+securityCall["resolution"]+"/"

	elif "forex" in securityCall:
		outname = ticker.lower() + ".csv"
		zipname = ticker.lower() + ".zip"

		outdir = "../Data/forex/" + securityCall["marketName"]+ "/" + securityCall["resolution"] + "/"
	
	elif "options" in securityCall:
		dte = securityCall["startTime"].strftime("%Y%m%d")
		expdate = securityCall["symbolExpirationDate"].strftime("%Y%m%d")
	
		outname = dte + "_" + ticker.lower() + "_" + securityCall["resoultion"] + "_" + securityCall["tickType"] + "_" + securityCall["optionType"] + "_" 
		+ securityCall["optionStyle"] + "_" + securityCall["decicentStrikePrice"] + "_" + expdate + ".csv"

		zipname = dte + "_" + securityCall["tickType"] + "_" + securityCall["optionType"] + ".zip"
		
		outdir = "../Data/option/usa/" + securityCall["resoultion"] + "/" + ticker.lower() + "/"


	elif "futures" in equityCall:
		expdate = securityCall["symbolExpirationDate"].strftime("%Y%m%d")
		outname = ticker.lower() + "_" + securityCall["tickType"] + "_" + expdate + ".csv"
		zipname = ticker.lower() + "_" + securityCall["tickType"] + ".zip"
		
		outdir = "../Data/future/usa/" + securityCall["resolution"] + "/"
 
	
	elif "crpyto" in equityCall:
		outname = ticker.lower() + ".csv"
		zipname = ticker.lower() + "_" + securityCall["tickType"] + ".zip"
		
		outdir = "../Data/crpyto/" + securityCall["marketName"] + "/" + securityCall["resolution"] + "/" 



	if not os.path.exists(outdir):
	    os.makedirs(outdir)

	# Full path to equity csvzip
	fullname = os.path.join(outdir, outname)
	zipname = os.path.join(outdir, zipname)


	
	# Haewon Code start
	# Read csv and check if the data is arlady in it
	if os.path.exists(zipname):
		addname = os.path.join(outdir."add.csv")
	
		with ZipFile(zipname, 'r') as zip:
			zip.extract(fullname)


		df = getData(securityCall, ticker)
		print(df)
		df.to_csv(addname, header = False)
		fullname = pd.concat([fullname, addname])
		
		df = pd.read_csv(fullname)
		df = sorted(df, key=operator.itemgetter(0))

		df.drop_duplicates(subset=None, inplace = True)
		df.to_csv(fullname, header=False)
		os.remove(addname) 
	

	else:
		df = getData(securityCall, ticker)			
		print(df)
		df.to_csv(fullname, header=False)
		

	ZipFile(zipname, mode = "w").write(fullname, os.path.basename(fullname))
	os.remove(fullname)





def recieve():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    channel.queue_declare(queue="dataFetcher")

    channel.basic_consume(queue="dataFetcher", on_message_callback=callback, auto_ack=True)

    print("RECIEVER: [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


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
