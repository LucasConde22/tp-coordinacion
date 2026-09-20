import pika
import pika.exceptions
from .middleware import MessageMiddleware, MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

class _MessageMiddlewareRabbitMQ(MessageMiddleware):
    # Errores que indican desconexión:
    _DISCONNECTED_ERRORS = (
        pika.exceptions.AMQPConnectionError,
        pika.exceptions.ConnectionClosed,
        pika.exceptions.StreamLostError,
        pika.exceptions.ChannelWrongStateError,
        pika.exceptions.ConnectionWrongStateError,
        pika.exceptions.IncompatibleProtocolError,
        OSError, # Necesario por si el host que se pasa es inválido
    )

    _MSG_ERROR_CLOSED_CONNECTION = 'The connection has already been closed'
    _MSG_ERROR_CLOSED_CHANNEL = 'The channel is closed'

    def __init__(self, host):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.is_consuming = False
        self.queue_name = ''

    def start_consuming(self, on_message_callback):
        self._assert_connection_is_open()
        self._assert_channel_is_open()

        def callback(ch, method, properties, body):
            on_message_callback(body,
                                lambda: ch.basic_ack(method.delivery_tag),
                                lambda: ch.basic_nack(method.delivery_tag))

        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name,
                                        auto_ack=False,
                                        on_message_callback=callback)
            self.is_consuming = True
            self.channel.start_consuming()
        except self._DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
        finally:
            # Como start_consuming es bloqueante, cuando llega a
            # este punto es porque ya no está leyendo.
            self.is_consuming = False

    def stop_consuming(self):
        if not self.is_consuming:
            return
        
        self._assert_connection_is_open()
        self._assert_channel_is_open()

        try:
            self.channel.stop_consuming()
        except self._DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        finally:
            self.is_consuming = False

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e)

    def _publish(self, exchange, routing_key, message):
        try:
            self.channel.basic_publish(exchange=exchange,
                                       routing_key=routing_key,
                                       body=message)
        except self._DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)

    def _assert_connection_is_open(self):
        if not self.connection or self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError(self._MSG_ERROR_CLOSED_CONNECTION)
        
    def _assert_channel_is_open(self):
        if not self.channel or self.channel.is_closed:
            raise MessageMiddlewareDisconnectedError(self._MSG_ERROR_CLOSED_CHANNEL)

class MessageMiddlewareQueueRabbitMQ(_MessageMiddlewareRabbitMQ, MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        try:
            super().__init__(host)
            self.queue_name = queue_name
            self.channel.queue_declare(queue_name)
        except self._DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)

    def send(self, message):
        self._assert_connection_is_open()
        self._assert_channel_is_open()
        self._publish('', self.queue_name, message)

class MessageMiddlewareExchangeRabbitMQ(_MessageMiddlewareRabbitMQ, MessageMiddlewareExchange):
    _DIRECT_EXCHANGE_TYPE = 'direct'
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            super().__init__(host)
            self.exchange_name = exchange_name
            self.routing_keys = routing_keys
            self.channel.exchange_declare(
                        exchange=exchange_name,
                        exchange_type=self._DIRECT_EXCHANGE_TYPE)

            self.queue_name = self.channel\
                                        .queue_declare('', exclusive=True)\
                                        .method.queue

            for routing_key in routing_keys:
                self.channel.queue_bind(exchange=exchange_name,
                                        queue=self.queue_name,
                                        routing_key=routing_key)
            
        except self._DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)

    def send(self, message):
        self._assert_connection_is_open()
        self._assert_channel_is_open()
        for key in self.routing_keys:
            self._publish(self.exchange_name, key, message)
