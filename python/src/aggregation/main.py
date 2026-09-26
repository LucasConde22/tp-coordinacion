import os
import logging
import heapq
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amount_by_client_by_fruit = {}
        self.eof_by_client = {}

        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)
    
    def handle_shutdown(self, signum, frame):
        logging.info("Received shutdown signal")
        try:
            self.input_exchange.stop_consuming()
        except Exception as e:
            logging.error(f"Error while stopping consumption in input exchange: {e}")

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        amount_by_fruit = self._get_amount_by_fruit(client_id)
        current_item = amount_by_fruit.get(fruit, fruit_item.FruitItem(fruit, 0))
        amount_by_fruit[fruit] = current_item + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        self.eof_by_client[client_id] = self.eof_by_client.get(client_id, 0) + 1
        logging.info(f'Received {self.eof_by_client[client_id]}º EOF for client ID: {client_id}')
        if self.eof_by_client[client_id] != SUM_AMOUNT:
            return

        amount_by_fruit = self._get_amount_by_fruit(client_id)
        # nlargest usa un Min-Heap para sacar a los K mayores en tiempo O(N log K)
        top_items = heapq.nlargest(TOP_SIZE, amount_by_fruit.values())
        fruit_top = [(item.fruit, item.amount) for item in top_items]

        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))
        self.amount_by_client_by_fruit.pop(client_id, None)
        self.eof_by_client.pop(client_id, None)

    def _get_amount_by_fruit(self, client_id):
        return self.amount_by_client_by_fruit.setdefault(client_id, {})

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1:
            self._process_eof(*fields)
        ack()

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        finally:
            try:
                self.input_exchange.close()
                self.output_queue.close()
            except Exception as e:
                logging.error(f"Error while closing middlewares: {e}")


def main():
    logging.basicConfig(level=logging.INFO)
    try:
        aggregation_filter = AggregationFilter()
        aggregation_filter.start()
    except Exception as e:
        logging.error(f"Error in aggregation filter: {e}")
        return 1
    return 0


if __name__ == "__main__":
    main()
