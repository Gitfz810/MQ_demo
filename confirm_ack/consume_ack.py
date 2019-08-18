#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.exchange_declare(exchange='test_ch', exchange_type='direct')

channel.queue_declare(queue='test_queue', durable=True)

channel.queue_bind(queue='test_queue', exchange='test_ch', routing_key='confirmation')


# 定义一个消息确认函数，消费者成功处理完消息后会给队列发送一个确认信息，然后该消息会被删除
def ack_info_handler(channel, method, header, body):
    """ack_info_handler """
    print('ack_info_handler() called!')
    if body == 'quit':
        channel.basic_cancel(consumer_tag='hello_confirmation')
        channel.stop_sonsuming()
    else:
        print(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue='test_queue', on_message_callback=ack_info_handler, auto_ack=True, consumer_tag='hello_confirmation')

print('ready to consume msg...')
channel.start_consuming()
