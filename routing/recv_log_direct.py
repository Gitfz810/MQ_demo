#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pika
import sys


def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

# 创建queue 当与消费者断开连接的时候，队列被立即删除就销毁
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

severities = sys.argv[1:]
if not severities:
    sys.stderr.write("Usage: %s [info] [warning] [error]\n" % sys.argv[0])
    sys.exit(1)

for severity in severities:
    # 绑定queue与exchange
    channel.queue_bind(exchange='direct_logs', queue=queue_name, routing_key=severity)

print(' [*] Waiting for logs. To exit press CTRL+C')

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
