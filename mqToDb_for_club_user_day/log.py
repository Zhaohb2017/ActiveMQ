#!/usr/bin/python
# -*- coding: UTF-8 -*-
import time
import logging
from logging import handlers


class Logger(object):
    """
    实例化TimedRotatingFileHandler
    interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
    S 秒
    M 分
    H 小时、
    D 天、
    W 每星期（interval==0时代表星期一）
    midnight 每天凌晨
    """
    level_relations = {'debug': logging.DEBUG, 'info': logging.INFO, 'warning': logging.WARNING, 'error': logging.ERROR,
                       'crit': logging.CRITICAL}  # 日志级别关系映射
    # % (asctime)
    # s - % (pathname)
    # s[line: % (lineno)
    # d] -
    def __init__(self, filename, level='debug', when='D', backCount=3,
                 fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s \n'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)  # 设置日志格式
        self.logger.setLevel(self.level_relations.get(level))  # 设置日志级别
        sh = logging.StreamHandler()  # 往屏幕上输出
        sh.setFormatter(format_str)  # 设置屏幕上显示的格式
        th = handlers.TimedRotatingFileHandler(filename=filename, when=when, backupCount=2,
                                               encoding='utf-8')  # 往文件里写入#指定间隔时间自动生成文件的处理器
        th.setFormatter(format_str)  # 设置文件里写入的格式
        self.logger.addHandler(sh)  # 把对象加到logger里
        self.logger.addHandler(th)


if __name__ == '__main__':
    debug_file_name = '%s.debug.log' % (time.strftime("%Y%m%d"))
    error_file_name = '%s.error.log' % (time.strftime("%Y%m%d_%H%M%S"))
    debug_log = Logger(debug_file_name,fmt="")
    debug_log.logger.debug('debug')
    debug_log.logger.info('info')
    debug_log.logger.warning('警告')
    debug_log.logger.error('报错')
    debug_log.logger.critical('严重')

    error_log = Logger(error_file_name, level='error')
    error_log.logger.error('error')

