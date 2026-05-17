import json
from channels.generic.websocket import AsyncWebsocketConsumer


class KafkaDataConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add("Kafka_updates", self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("Kafka_updates", self.channel_name)
    
    async def send_kafka_data(self, event):
        message = event['data']
        await self.send(text_data=json.dumps({'message' : message}))