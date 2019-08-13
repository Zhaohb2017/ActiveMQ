#!/usr/bin/python
# -*- coding: UTF-8 -*-
import MySQLdb
import MySQLdb.cursors
from log import Logger
import yaml,io
import  time
conf_path = "./conf.yaml"
def get_conf_data():
    _file_data = io.open(conf_path, 'r', encoding='utf-8')
    conf_data = yaml.load(_file_data)
    _file_data.close()
    return conf_data
conf_data=get_conf_data()
conf_data = conf_data["DB"]
debug_file_name = '%s.debug.log' % (time.strftime("%Y%m%d"))
debug_log = Logger(debug_file_name)
def con(db_list):
    conn = MySQLdb.connect(host=conf_data['host'], port=conf_data['port'], user=conf_data['user'],
                                passwd=conf_data['pwd'], db=conf_data['db'],
                                charset='utf8', cursorclass=MySQLdb.cursors.DictCursor)
    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()

    # 使用execute方法执行SQL语句
    for i in db_list:
        sql = """
			SELECT
				*
			FROM
				majiangcs_log_mq.club_user_day3_%d AS A
			LEFT OUTER JOIN majiangcs_log.club_user_day3_%d AS B ON A.mid = B.mid
			AND A.playtotal = B.playtotal
			AND A.croom = B.croom
			AND A.playcount = B.playcount
			AND A.jointotal = B.jointotal
			AND A.joinroom = B.joinroom
			AND A.integral = B.integral
			AND A.winner = B.winner
			AND A.room_type = B.room_type
			AND A.integraltotal = B.integraltotal
			AND A.winnertotal = B.winnertotal
			AND A.winnerscore = B.winnerscore
			AND A.winnertotalscore = B.winnertotalscore
			AND A.colltype = B.colltype
			AND A.paytotal = B.paytotal
			AND A.payreal = B.payreal
			AND A.paytotal_valid = B.paytotal_valid
			AND A.payreal_valid = B.payreal_valid
			WHERE
				A.playtotal IS NULL
			OR B.playtotal IS NULL
			OR A.croom IS NULL
			OR B.croom IS NULL
			OR A.playcount IS NULL
			OR B.playcount IS NULL
			OR A.jointotal IS NULL
			OR B.jointotal IS NULL
			OR A.joinroom IS NULL
			OR B.joinroom IS NULL
			OR A.integral IS NULL
			OR B.integral IS NULL
			OR A.winner IS NULL
			OR B.winner IS NULL
			OR A.room_type IS NULL
			OR B.room_type IS NULL
			OR A.integraltotal IS NULL
			OR B.integraltotal IS NULL
			OR A.winnertotal IS NULL
			OR B.winnertotal IS NULL
			OR A.winnerscore IS NULL
			OR B.winnerscore IS NULL
			OR A.winnertotalscore IS NULL
			OR B.winnertotalscore IS NULL
			OR A.colltype IS NULL
			OR B.colltype IS NULL
			OR A.paytotal IS NULL
			OR B.paytotal IS NULL
			OR A.payreal IS NULL
			OR B.payreal IS NULL
			OR A.paytotal_valid IS NULL
			OR B.paytotal_valid IS NULL
			OR A.payreal_valid IS NULL
			OR B.payreal_valid IS NULL;"""%(i,i)

        cursor.execute(sql)

        # 使用 fetchone() 方法获取一条数据
        data = cursor.fetchall()
        debug_log.logger.debug(data)


    # 关闭数据库连接
    conn.close()
db_list = [i for i in range(10)]
con(db_list)
