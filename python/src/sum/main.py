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
GATEWAY_EOF_MESSAGE_FIELDS = 2
CONTROL_EOF_MESSAGE_FIELDS = 1
REPORT_EOF_MESSAGE_FIELDS = 2
REPORT_EOF_TO_AGGREGATORS_FLAG = -1
INITIAL_FRUIT_AMOUNT = 0
ENCODING = "utf-8"
THREADS_TIMEOUT_TIME = 3 # Definido por las dudas, aunque el thread hijo finaliza en menos tiempo.

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
        self.messages_count_by_client = {}
        self.received_eof = {}
        self.client_ids_to_report = set()
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        logging.info("Received shutdown signal")
        try:
            self.input_queue.stop_consuming()
            self.control_receiver.threadsafe_stop_consuming()
        except Exception as e:
            logging.error(f"Error while stopping consumption: {e}")

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        self._increment_messages_count(client_id)
        amount_by_fruit = self._get_amount_by_fruit(client_id)
        amount_by_fruit[fruit] = amount_by_fruit.get(fruit,\
            fruit_item.FruitItem(fruit, INITIAL_FRUIT_AMOUNT)) + fruit_item.FruitItem(fruit, int(amount))

        if client_id in self.client_ids_to_report:
            self._send_data_to_aggregators(client_id)

    def _manage_eof_message(self, client_id, total_messages):
        logging.info(f"Received EOF for client ID: {client_id} with total amount of messages: {total_messages}")
        self.received_eof[client_id] = (total_messages, 0)
        self._broadcast_eof_to_sums(client_id)

    def _broadcast_eof_to_sums(self, client_id):
        logging.info(f"Broadcasting EOF message for client ID {client_id} to other sums")
        self.control_sender.send(message_protocol.internal.serialize([client_id]))

    def _broadcast_eof_completition(self, client_id):
        logging.info(f"Broadcasting EOF completition message for client ID {client_id} to other sums")
        self.control_sender.send(message_protocol.internal.serialize([client_id, REPORT_EOF_TO_AGGREGATORS_FLAG]))

    def _process_eof(self, client_id):
        self.client_ids_to_report.add(client_id)
        self._send_data_to_aggregators(client_id)

    def _process_eof_completion(self, client_id):
        logging.info(f"Processing EOF completion for client ID: {client_id}")
        self.client_ids_to_report.discard(client_id)

        logging.info(f"Broadcasting EOF message to aggregators for client ID {client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def _send_data_to_aggregators(self, client_id):
        logging.info(f"Sending data messages to addecuate aggregators")

        amount_by_fruit = self._get_amount_by_fruit(client_id)
        for final_fruit_item in amount_by_fruit.values():
            aggregator_index = self._get_aggregator_for_fruit(final_fruit_item.fruit)
            self.data_output_exchanges[aggregator_index].send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        self.amount_by_client_by_fruit.pop(client_id, None)
        messages_count = self.messages_count_by_client.pop(client_id, 0)
        self._report_amount_of_received_messages(client_id, messages_count)

    def _report_amount_of_received_messages(self, client_id, messages_amount):
        logging.info(f"Reporting amount of received messages for client ID {client_id}: {messages_amount}")
        self.control_sender.send(message_protocol.internal.serialize([client_id, messages_amount]))

    def _get_amount_by_fruit(self, client_id):
        return self.amount_by_client_by_fruit.setdefault(client_id, {})

    def _increment_messages_count(self, client_id):
        self.messages_count_by_client[client_id] = self.messages_count_by_client.get(client_id, 0) + 1

    def _get_aggregator_for_fruit(self, fruit):
        hashed_fruit = zlib.adler32(fruit.encode(ENCODING))
        return hashed_fruit % AGGREGATION_AMOUNT

    def _update_received_messages_amount(self, client_id, new_messages_amount):
        if not client_id in self.received_eof:
            return
        
        total_messages, received_messages_amount = self.received_eof[client_id]
        updated_amount = received_messages_amount + new_messages_amount
        self.received_eof[client_id] = (total_messages, updated_amount)
        logging.info(f"[Coordinator for client {client_id}] Increased received messages amount by {updated_amount}")

        if updated_amount >= total_messages:
            self.received_eof.pop(client_id, None)
            self._broadcast_eof_completition(client_id)

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == DATA_MESSAGE_FIELDS:
            with self.lock:
                self._process_data(*fields)
        elif len(fields) == GATEWAY_EOF_MESSAGE_FIELDS:
            with self.lock:
                self._manage_eof_message(*fields)
        ack()

    def process_eof_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == CONTROL_EOF_MESSAGE_FIELDS:
            with self.lock:
                self._process_eof(*fields)
        elif len(fields) == REPORT_EOF_MESSAGE_FIELDS:
            with self.lock:
                if fields[1] == REPORT_EOF_TO_AGGREGATORS_FLAG:
                    self._process_eof_completion(fields[0])
                else:
                    self._update_received_messages_amount(*fields)
        ack()

    def start(self):
        t = None
        try:
            t = threading.Thread(
                target=lambda: self.control_receiver.start_consuming(self.process_eof_message),
                daemon=True # Marcado como deamon por las dudas, de todas formas se hace el join.
            )
            t.start()
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            try:
                if t is not None and t.is_alive():
                    t.join(timeout=THREADS_TIMEOUT_TIME)
            except Exception as e:
                logging.error(f"Error while joining control receiver thread: {e}")
            try:
                self.input_queue.close()
                for data_output_exchange in self.data_output_exchanges:
                    data_output_exchange.close()
                self.control_sender.close()
                self.control_receiver.close()
            except Exception as e:
                logging.error(f"Error while closing middlewares: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    try:
        logging.info("Starting sum filter")
        sum_filter = SumFilter()
        sum_filter.start()
    except Exception as e:
        logging.error(f"Error in sum filter: {e}")
        return 1
    return 0


if __name__ == "__main__":
    main()
