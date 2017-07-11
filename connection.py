from datetime import datetime

import psycopg2
import logging
import sys

log_filename = "log_{}.log".format(datetime.now().strftime('%d-%m-%Y'))
logger       = logging.getLogger('converter')
hdlr         = logging.FileHandler(log_filename)
formatter    = logging.Formatter('%(asctime)s %(levelname)s %(massage)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)


class Connection():
    
    _db=None
    def __init__(self,nhost,db,usr,pswd):
        try:
            self._db = psycopg2.connect(
                host=nhost,
                database=db,
                user=usr,
                password=pswd
            )

        except Exception as ex:
            logger.error("Connection error __init__")
            logger.error(ex)
            print("Connection error.")
            sys.exit(-1)

    def insertion(self,sql,tup):
        try:  
            cur = self._db.cursor()
            cur.executemany(sql,tup)
            cur.close()
            self._db.commit()

        except Exception as ex:
            logger.error(ex)
            self._db.rollback()
            return False

        return True

    def manipulate(self,sql):
        try:
            cur = self._db.cursos()
            cur.execute(sql)
            cur.close()
            self._db.commit()

        except Exception as ex:
            logger.error(ex)
            self._db.rollback()
            print('Erro on manipulation')
            return False

        return True

    def query(self,sql):
        rs=None

        try:
            cur=self._db.cursor()
            cur.execute(sql)
            rs=cur.fetchall()

        except Exception as ex:
            logger.error(ex)
            self._db.rollback()
            return None

        return rs

    def close(self):
        try:
            self._db.close()

        except Exception as ex:
            logger.error(ex)
            return None

        return ">>>closed"