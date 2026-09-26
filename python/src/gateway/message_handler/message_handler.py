from common import message_protocol
import uuid

RESULT_MESSAGE_FIELDS = 2


class MessageHandler:

    def __init__(self):
        self.id = str(uuid.uuid4())
        self.sent_messages = 0
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.sent_messages += 1
        return message_protocol.internal.serialize([self.id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.id, self.sent_messages])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if not fields or len(fields) != RESULT_MESSAGE_FIELDS:
            return None
        
        recv_id, result = fields[0], fields[1]
        
        if recv_id == self.id:
            return result
        return None
