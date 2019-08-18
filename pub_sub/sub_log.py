#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import pika


def callback(ch, method, properties, body):
    print(" [x] %r" % body)


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
# 指定exchange的名字与类型; 类型有 direct topic headers fanout
channel.exchange_declare(exchange='logs', exchange_type='fanout')

# 声明一个名字的的queue，并且当最后一个消费者断开连接后，该queue销毁
result = channel.queue_declare(queue='', exclusive=True)
# 获取queue的名字
queue_name = result.method.queue
# 绑定exchange与queue
channel.queue_bind(exchange='logs', queue=queue_name)

print(' [*] Waiting for logs. To exit press CTRL+C')

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
