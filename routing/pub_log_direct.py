#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
# 创建名为direct_logs的exchange exchange_type为direct
channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

severity = sys.argv[1] if len(sys.argv) > 2 else 'info'
message = ' '.join(sys.argv[2:]) or 'Hello World!'
# 发送message
channel.basic_publish(exchange='direct_logs', routing_key=severity, body=message)

print(" [x] Sent %r:%r" % (severity, message))
connection.close()
