#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import sys

import pika


# 定义消息发布后publisher接受到的确认信息处理函数
def confirm_handler(frame):
    if type(frame.method) == pika.spec.Confirm.SelectOk:
        """生产者创建的channel处于‘publisher confirmation’模式"""
        print('Channel in "confirm" mode!')
    elif type(frame.method) == pika.spec.Basic.Nack:
        """生产者接受到消息发送失败并且消息丢失的消息"""
        print('Message lost!')
    elif type(frame.method) == pika.spec.Basic.ack:
        if frame.method.delivery_tag in msg_ids:
            """生产者接受到成功发布的消息"""
            print('Confirm received!')
            msg_ids.remove(frame.method.delivery_tag)


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

# Turn on delivery confirmations
channel.confirm_delivery(callback=confirm_handler)

# create exchange
channel.exchange_declare(exchange='test_ch', exchange_type='direct')

# msg info
msg = sys.argv[1]
# msg properties 消息属性
msg_props = pika.BasicProperties()
msg_props.content_type = 'text/plain'
# 持久化
msg_props.delivery_mode(2)
msg_ids = []

channel.basic_publish(body=msg, exchange='test_ch', properties=msg_props, routing_key='confirmation')
print('Published!')

msg_ids.append(len(msg_ids) + 1)

connection.close()
