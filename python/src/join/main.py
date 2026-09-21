import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


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

    def _update_partial_fruit_tops_for_client(self, client_id, partial_fruit_top):
        client_tops = self._get_client_tops_for(client_id)
        for item in partial_fruit_top:
            fruit, amount = item[0], item[1]
            client_tops.append(fruit_item.FruitItem(fruit, amount))

        self.eof_by_client[client_id] = self.eof_by_client.get(client_id, 0) + 1
        logging.info(
            f"Received {self.eof_by_client[client_id]}º partial top for client ID: {client_id}"
        )

    def _send_final_top(self, client_id):
        logging.info(f"All partial tops were received for client ID: {client_id}")
        client_tops = self._get_client_tops_for(client_id)
        client_tops.sort(reverse=True)
        final_fruit_chunk = client_tops[:TOP_SIZE]
        final_top = [(item.fruit, item.amount) for item in final_fruit_chunk]
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_top])
        )
        self.fruit_tops_by_client.pop(client_id, None)
        self.eof_by_client.pop(client_id, None)

    def _get_client_tops_for(self, client_id):
        return self.fruit_tops_by_client.setdefault(client_id, [])

    def process_messsage(self, message, ack, nack):
        logging.info("Received partial top")
        fields = message_protocol.internal.deserialize(message)
        if not fields or len(fields) != 2:
            ack()
            return
        client_id, partial_fruit_top = fields[0], fields[1]

        self._update_partial_fruit_tops_for_client(client_id, partial_fruit_top)

        if self.eof_by_client[client_id] == AGGREGATION_AMOUNT:
            self._send_final_top(client_id)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
