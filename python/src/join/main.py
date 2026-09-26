import os
import logging
import signal
import heapq

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])
 
PARTIAL_TOP_MESSAGE_FIELDS = 2
INITIAL_FRUIT_AMOUNT = 0


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_tops_by_client = {}
        self.eof_by_client = {}
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        logging.info("Received shutdown signal")
        try:
            self.input_queue.stop_consuming()
        except Exception as e:
            logging.error(f"Error while stopping consumption in input queue: {e}")

    def _update_partial_fruit_tops_for_client(self, client_id, partial_fruit_top):
        client_fruits = self._get_client_fruits_for(client_id)
        for fruit, amount in partial_fruit_top:
            current_item = client_fruits.get(fruit, fruit_item.FruitItem(fruit, INITIAL_FRUIT_AMOUNT))
            client_fruits[fruit] = current_item + fruit_item.FruitItem(fruit, int(amount))

        self.eof_by_client[client_id] = self.eof_by_client.get(client_id, 0) + 1
        logging.info(
            f"Received {self.eof_by_client[client_id]}º partial top for client ID: {client_id}"
        )

    def _send_final_top(self, client_id):
        logging.info(f"All partial tops were received for client ID: {client_id}")
        client_fruits = self._get_client_fruits_for(client_id)
        top_items = heapq.nlargest(TOP_SIZE, client_fruits.values())
        final_top = [(item.fruit, item.amount) for item in top_items]
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_top])
        )
        self.fruit_tops_by_client.pop(client_id, None)
        self.eof_by_client.pop(client_id, None)

    def _get_client_fruits_for(self, client_id):
        return self.fruit_tops_by_client.setdefault(client_id, {})

    def process_messsage(self, message, ack, nack):
        logging.info("Received partial top")
        fields = message_protocol.internal.deserialize(message)
        if not fields or len(fields) != PARTIAL_TOP_MESSAGE_FIELDS:
            ack()
            return
        client_id, partial_fruit_top = fields[0], fields[1]

        self._update_partial_fruit_tops_for_client(client_id, partial_fruit_top)

        if self.eof_by_client[client_id] == AGGREGATION_AMOUNT:
            self._send_final_top(client_id)
        ack()

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            try:
                self.input_queue.close()
                self.output_queue.close()
            except Exception as e:
                logging.error(f"Error while closing queues: {e}")


def main():
    logging.basicConfig(level=logging.INFO)
    try:
        logging.info("Starting join filter")
        join_filter = JoinFilter()
        join_filter.start()
    except Exception as e:
        logging.error(f"Error in join filter: {e}")
        return 1

    return 0


if __name__ == "__main__":
    main()
