#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import sys

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

message = ' '.join(sys.argv[1:]) or "Hello World!"
channel.basic_publish(
    exchange='',
    # 匹配相应队列
    routing_key='task_queue',
    body=message,
    # make msg persistent
    properties=pika.BasicProperties(delivery_mode=2)
)
print("[x] send msg: %r" % message)

connection.close()
