import os
import logging
import threading
import zlib
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
CONTROL_ROUTING_KEY = "CONTROL_ROUTING_KEY"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

DATA_MESSAGE_FIELDS = 3
EOF_MESSAGE_FIELDS = 1
INITIAL_FRUIT_AMOUNT = 0
ENCODING = "utf-8"
THREADS_TIMEOUT_TIME = 3

class SumFilter:
    def __init__(self):
        self.lock = threading.Lock()
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.control_receiver = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [CONTROL_ROUTING_KEY]
        )
        self.control_sender = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [CONTROL_ROUTING_KEY]
        )

        self.amount_by_client_by_fruit = {}
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        logging.info("Received shutdown signal")
        try:
            self.input_queue.stop_consuming()
            self.control_receiver.stop_consuming()
        except Exception as e:
            logging.error(f"Error while stopping consumption: {e}")

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        amount_by_fruit = self._get_amount_by_fruit(client_id)
        amount_by_fruit[fruit] = amount_by_fruit.get(fruit,\
            fruit_item.FruitItem(fruit, INITIAL_FRUIT_AMOUNT)) + fruit_item.FruitItem(fruit, int(amount))

    def _broadcast_eof_to_sums(self, client_id):
        logging.info(f"Broadcasting EOF message for client ID {client_id} to other sums")
        self.control_sender.send(message_protocol.internal.serialize([client_id]))

    def _process_eof(self, client_id):
        logging.info(f"Sending data messages to addecuate aggregators")

        amount_by_fruit = self._get_amount_by_fruit(client_id)
        for final_fruit_item in amount_by_fruit.values():
            aggregator_index = self._get_aggregator_for_fruit(final_fruit_item.fruit)
            self.data_output_exchanges[aggregator_index].send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))
            
        self.amount_by_client_by_fruit.pop(client_id, None)

    def _get_amount_by_fruit(self, client_id):
        return self.amount_by_client_by_fruit.setdefault(client_id, {})

    def _get_aggregator_for_fruit(self, fruit):
        hashed_fruit = zlib.adler32(fruit.encode(ENCODING))
        return hashed_fruit % AGGREGATION_AMOUNT

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == DATA_MESSAGE_FIELDS:
            with self.lock:
                self._process_data(*fields)
        elif len(fields) == EOF_MESSAGE_FIELDS:
            self._broadcast_eof_to_sums(*fields)
        ack()

    def process_eof_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == EOF_MESSAGE_FIELDS:
            with self.lock:
                self._process_eof(*fields)
        ack()

    def start(self):
        t = None
        try:
            t = threading.Thread(
                target=lambda: self.control_receiver.start_consuming(self.process_eof_message),
                daemon=True
            )
            t.start()
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            try:
                self.input_queue.close()
                for data_output_exchange in self.data_output_exchanges:
                    data_output_exchange.close()
                self.control_sender.close()
                self.control_receiver.close()
            except Exception as e:
                logging.error(f"Error while closing middlewares: {e}")
            try:
                if t is not None:
                    t.join(timeout=THREADS_TIMEOUT_TIME)
            except Exception as e:
                logging.error(f"Error while joining control receiver thread: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
