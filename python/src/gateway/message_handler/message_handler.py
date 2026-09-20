from common import message_protocol
import uuid

class MessageHandler:

    def __init__(self):
        self.id = uuid.uuid4()
        pass
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if not fields:
            return None
        
        recv_id, result = fields[0], fields[1]
        
        if recv_id == self.id:
            return result
        return None
