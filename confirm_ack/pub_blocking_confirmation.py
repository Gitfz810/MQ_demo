#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import sys

import pika


if __name__ == '__main__':
    # msg info
    msg = sys.argv[1]
    # msg properties 消息属性
    msg_props = pika.BasicProperties()
    msg_props.content_type = 'text/plain'
    # 持久化
    msg_props.delivery_mode(2)
    msg_ids = []
    # 同步回调 阻塞
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

    channel = connection.channel()

    # create exchange
    channel.exchange_declare(exchange='test_ch', exchange_type='direct', durable=True)
    # Turn on delivery confirmations
    channel.confirm_delivery()
    if channel.basic_publish(body=msg, exchange='test_ch', properties=msg_props, routing_key='confirmation', mandatory=True):
        print('Published!')
        msg_ids.append(len(msg_ids) + 1)
    else:
        print('Message lost!')

    connection.close()
