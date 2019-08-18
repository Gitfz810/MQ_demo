#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time

import pika


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(body.count(b'.'))
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

# 公平调度，同一时刻，只给同一个worker 一个任务
channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_message_callback=callback, queue='task_queue')

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
