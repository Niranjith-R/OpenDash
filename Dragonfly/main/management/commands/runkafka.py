from django.core.management.base import BaseCommand
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import os


kafka_host = os.getenv("kafka_server")


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        channel_layer = get_channel_layer()

        conf = {
        'bootstrap.servers' : kafka_host,
        'group.id' : 'test',
        'auto.offset.reset' : 'earliest'
        }

        consumer = Consumer(conf)
        topic = ["vid_recorder", "Storage"]
        consumer.subscribe(topic)

        while True:
            msg = consumer.poll(timeout = 1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
                
            else:
                msg_value = msg.value()
                decoded_value = msg_value.decode("utf-8")


                async_to_sync(channel_layer.group_send)(
                    "Kafka_updates", {
                        "type": "send_kafka_data",
                        "data": decoded_value
                    }
                )