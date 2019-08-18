#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import sys

import pika


# 定义消息发布后publisher接受到的确认信息处理函数
def confirm_handler(frame):
    if isinstance(frame.method, pika.spec.Confirm.SelectOk):
        """生产者创建的channel处于‘publisher confirmation’模式"""
        print('Channel in "confirm" mode!')
    elif isinstance(frame.method, pika.spec.Basic.Nack):
        """生产者接受到消息发送失败并且消息丢失的消息"""
        print('Message lost!')
    elif isinstance(frame.method, pika.spec.Basic.Ack):
        if frame.method.delivery_tag in msg_ids:
            """生产者接受到成功发布的消息"""
            print('Confirm received!')
            msg_ids.remove(frame.method.delivery_tag)


def on_open(conn):
    conn.channel(on_open_callback=on_channel_open)


def on_channel_open(channel):
    channel = connection.channel()

    # create exchange
    channel.exchange_declare(exchange='test_ch', exchange_type='direct')
    # Turn on delivery confirmations
    channel.confirm_delivery(ack_nack_callback=confirm_handler)
    channel.basic_publish(body=msg, exchange='test_ch', properties=msg_props, routing_key='confirmation')


if __name__ == '__main__':
    # msg info
    msg = sys.argv[1]
    # msg properties 消息属性
    msg_props = pika.BasicProperties()
    msg_props.content_type = 'text/plain'
    # 持久化
    msg_props.delivery_mode(2)
    msg_ids = []
    # 异步回调
    connection = pika.SelectConnection(pika.ConnectionParameters(host='localhost'), on_open_callback=on_open)

    print('Published!')

    msg_ids.append(len(msg_ids) + 1)

    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
        connection.ioloop.start()
