#!/usr/bin/python
# -*- coding: UTF-8 -*-

import time
import os, signal,socket
import stomp
import multiprocessing
import db
from sqlmaker import *
import yaml
import io

conf_path = "./conf.yaml"
def get_conf_data():
    _file_data = io.open(conf_path, 'r', encoding='utf-8')
    conf_data = yaml.load(_file_data)
    _file_data.close()
    return conf_data
conf_data = get_conf_data()


def writePid():
    pid = str(os.getpid())
    f = open('server.pid', 'w')
    f.write(pid)
    f.close()



def Handler(signum, frame):
    debug_log.logger.debug('terminate process %d' % os.getpid())

    try:
        debug_log.logger.debug('the processes is %s' % ps)

        for p in ps:
            debug_log.logger.debug('process %d terminate' % p.pid)

            p.terminate()

    except Exception as e:
        debug_log.logger.debug(e)


class SampleListener(object):
    def __init__(self, conn):
        self.conn = conn
        self.db = db.DB(conf_data['DB'])
        self.udpconnect = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.SQLMaker = SQLMaker()

    #   发送
    def sendToUdp(self, span, msg, dirname, type="dberror"):
        data = {}
        data["span"] = span
        data["msg"] = msg.strip()
        data["dir"] = dirname
        data["type"] = type
        self.udpconnect.connect((conf_data['udp']['host'], int(conf_data['udp']['port'])))
        self.udpconnect.sendall(json.dumps(data))

    def on_error(self, headers, message):
        debug_log.logger.debug('$$$ received an error: %s from MQ' % message)

        self.sendToUdp(0, '$$$ received an error: %s from MQ', '')

    def on_message(self, headers, message):
        debug_log.logger.debug('headers: %s ' % (headers))
        debug_log.logger.debug('message: %s %s' % (message, time.time()))

        # try:
        #     maker = DB.connect(conf_data["DB"])
        #     debug_log.logger.debug("maker%s"%maker)
        # except Exception as err:
        #     self.sendToUdp(0, message + str(err), '')
        # else:
        debug_log.logger.debug("~"*10)
        json_msg = self.SQLMaker.msg_transform(message)
        result,sql = self.SQLMaker.data_structure_analysis(message=json_msg,DB=self.db,dbname=conf_data['DB']['db'])
        debug_log.logger.debug("result: %s        sql:%s"%(result,sql))
        if result != True:
            debug_log.logger.debug("#" * 10)
            self.conn.ack(id=headers['message-id'], subscription=headers['subscription'])  # 消费消息记录
        else:  # 失败
            sendstr = ''
            sendstr += str("error:") + '.' + sql + '<br/>'
            debug_log.logger.debug(sendstr)
            self.sendToUdp(0, sendstr, '')

    def on_disconnected(self):
        print('disconnected')
        self.sendToUdp(0, 'disconnected', '')
        debug_log.logger.debug('disconnected')

#        connect_and_subscribe(self.conn)

##从队列接收消息
def receive_from_queue():
    conn = stomp.Connection([(conf_data['mq']['host'], conf_data['mq']['port'])])
    conn.set_listener(listener_name, SampleListener(conn))  # 注册消息监听者，异步
    connect_and_subscribe(conn, queue_name)

    while True:
        try:
            time.sleep(1)
        except:
            break
    conn.disconnect()


# ##从主题接收消息
# def receive_from_topic():
#     conn = stomp.Connection10([(conf_data['mq']['host'], conf_data['mq']['port'])], heartbeats=(4000, 4000))
#     conn.set_listener(listener_name, SampleListener(conn))
#     connect_and_subscribe(conn, topic_name)
#     while 1:
#         send_to_topic('topic')
#         time.sleep(3)  # secs
#
#     conn.disconnect()


def connect_and_subscribe(conn, dest):
    conn.start()
    conn.connect(wait=True)
    conn.subscribe(destination=dest, id=1, ack='client')  # 开始监听接收消息


if __name__ == '__main__':

    queue_name = conf_data['queue_Xlogger']
    listener_name = conf_data['conlistener_name']

    # send_to_queue('len 123')
    # receive_from_queue()

    # receive_from_topic()

    debug_log.logger.debug("The number of CPU is:" + str(multiprocessing.cpu_count()))
    count = conf_data['process']
    ps = []
    # 创建子进程实例
    for i in range(count):
        p = multiprocessing.Process(target=receive_from_queue, name="worker" + str(i), args=())
        ps.append(p)

    # 开启进程
    for i in range(count):
        ps[i].daemon = True  # 因子进程设置了daemon属性为True，主进程正常结束，它们就随着结束了。但主进程是kill掉的，就不会
        ps[i].start()
        debug_log.logger.debug("p.pid:%s"%ps[i].pid)

        debug_log.logger.debug("p.name: %s"%ps[i].name)
        debug_log.logger.debug("p.is_alive: %s"%ps[i].is_alive())

    signal.signal(signal.SIGTERM, Handler)
    # time.sleep(10)
    # while True:
    #     try:
    #         time.sleep(1)
    #     except:
    #         break
    # 阻塞进程
    for i in ps:
        i.join()
