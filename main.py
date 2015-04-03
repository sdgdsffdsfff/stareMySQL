from gevent import monkey, sleep
monkey.patch_all()
import pymysql
import gevent
from settings import *
import logging

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='myapp.log',
                filemode='a')

def init_mysqldb(host,port,name,passwd,mysqldb):
    conn = pymysql.connect(host=host,user=name,passwd=passwd,db=mysqldb)
    cur = conn.cursor()

    return cur

mysqldbs = {}
for dbname, db in DBS.iteritems():
    mysqldbs[dbname] = init_mysqldb(host=db["host"],port=db["port"],name=db["user"],passwd=db["passwd"],mysqldb="test")


def task(cursor):
    while True:
        cursor.execute( "select id,info from information_schema.PROCESSLIST where time>%d;" % TIMEOUT)
        for i in cursor.fetchall():
            id = i[0]
            sql = i[1]
            logging.info("find %d timeout, sql: %s" % (id, sql))
            cursor.execute( "kill %d;" % id)
            logging.info("%d is killed with sql: %s" % (id, sql))
        sleep(10)

def main():
    jobs = [gevent.spawn(task, db) for dbname, db in mysqldbs.iteritems()]
    gevent.wait(jobs)

if __name__ == "__main__":
    main()
