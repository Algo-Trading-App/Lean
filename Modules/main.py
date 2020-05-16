import json
import sys

from DataFetcher.main import DataFetcher
from PortfolioGenerator.main import PortfolioGenerator

# Quandl API Key
API_KEY = "b5wYcieS5ZYxGvJNN7yW"

def main():
    # Load data from query JSON
    with open("PortfolioGenerator/message/PortfolioGeneratorQuery.json", "r") as jsonQuery:
        message = jsonQuery
        message = json.loads(message.read())


    if ("-d" in sys.argv):
        fetcher = DataFetcher()
        fetcher.process(message)

    if ("-g" in sys.argv):
        generator = PortfolioGenerator(API_KEY)
        generator.send(message, "backtest", "backtest")
        generator.send(message, "backtestTrigger", "backtestTrigger")
        generator.send(message, "backtest", "backtest")
        generator.send(message, "backtestTrigger", "backtestTrigger")
        generator.recieve()


if __name__ == "__main__":
    main()
