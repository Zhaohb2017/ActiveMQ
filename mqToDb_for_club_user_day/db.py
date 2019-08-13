#!/usr/bin/python
# -*- coding: UTF-8 -*-
import MySQLdb
import MySQLdb.cursors


class DB:
    #----------------------------------------------------------------------
    def __init__(self,conf_data):
        self.connect(conf_data)

    def connect(self,conf_data):
        self.conn = MySQLdb.connect(host=conf_data['host'],port=conf_data['port'],user=conf_data['user'], passwd=conf_data['pwd'],db=conf_data['db'],
        	charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
    #----------------------------------------------------------------------
    def updateMulti(self,sql):#error = db.update(insert_sql,arg) 插入或更新，成功返回Flase，失败返回错误信息

        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
            cds = cur.fetchall()
            error = False
        except (AttributeError, MySQLdb.OperationalError),e:
            error = e.args
            self.connect()
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
            error = False
        except MySQLdb.Error,e:
            self.conn.rollback()
            error = e.args
        finally:
            return error
    #----------------------------------------------------------------------
    def query(self, sql):#[cur, error] = db.query(query_sql) 查询，成功返回cursor，失败返回错误信息
        cur = None
        error = True
        try:
            cur = self.conn.cursor()
            a = cur.execute(sql)
            error = False
        except (AttributeError, MySQLdb.OperationalError), e:
            error = e.args
            self.connect()
            cur = self.conn.cursor()
            cur.execute(sql)
            error = False
        except MySQLdb.Error,e:
            error = e.args
        finally:
            return [cur,error]
    #----------------------------------------------------------------------
    def close(self):
        try:
            if self.conn:
                self.conn.close()
        except:
            pass

if __name__ == "__main__":
    pass


