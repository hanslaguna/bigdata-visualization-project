# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('counts')

consumer = KafkaConsumer(
    'counts',         # kafka topic name
    bootstrap_servers = ['localhost:9092'],
    auto_offset_reset = 'latest',
    enable_auto_commit = True
    )

for message in consumer:
    message = message.value
    messagestring = message.decode("utf-8")    
    insertquery = "INSERT INTO counting JSON'" + messagestring + "';"
    print(message)
    print("\n")
    session.execute(insertquery)