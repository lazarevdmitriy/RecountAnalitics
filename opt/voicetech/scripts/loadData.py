#!/usr/bin/python3
# -*- coding: utf-8 -*-
#---------------------------------------------------------------------
# Elasticsearch loading data
#
# Version      Date        Info
# 1.0          2022        Initial Version
#
# Made by Dmitriy Lazarev GOLDLINUX Copyleft (c) 2022
#---------------------------------------------------------------------
# python3 -m pip install mysql-connector elasticsearch

import sys, datetime, json, mysql.connector
from elasticsearch import Elasticsearch

elastic_host="http://localhost:9200"
index='phone_cdr'

def main():

    query = f"""
    SELECT
    	interactionID,
    	srcAnnotation,
    	dstAnnotation,
    	DATE_FORMAT(calldate , '%Y-%m-%dT%H:%i:%s') as '@timestamp',
    	dataset_id,
    	IF(dst = isManager, 'in', 'out') as direction
    FROM
    	phone_cdr
    WHERE
        date(calldate) BETWEEN '{start}' AND '{stop}'
    GROUP BY
    	interactionID;
        """

    cursor.execute(query)
    row = cursor.fetchone()
    while row is not None:
        try:
            resp = es.index(index=index, id=row['interactionID'], document=row)
            print(resp['result'], row['interactionID'], row['@timestamp'])
        except Exception as e:
            print(e)
        row = cursor.fetchone()

if __name__ == '__main__':
    if len(sys.argv) < 2:
      print("\nYou didn't give arguments!\n"
      "Examle: ./loadData.py '2022-02-24'")
      sys.exit(0)
    elif len(sys.argv) < 3:
        from_date = date_to = sys.argv[1]
    else:
        _, from_date, date_to = sys.argv
    start = datetime.datetime.strptime(f"{from_date} 00:00:00",'%Y-%m-%d %H:%M:%S')
    stop = datetime.datetime.strptime(f"{date_to} 23:59:59",'%Y-%m-%d %H:%M:%S')

    try:
        with open("/opt/voicetech/config/mysql.conf") as f:
            config = json.load(f)
            host=config['host']
            db=config['base']
            user=config['user']
            passwd=config['pass']
    except Exception as e:
        sys.exit(0)

    es = Elasticsearch(elastic_host)
    mydb = mysql.connector.connect(host=host, user=user, passwd=passwd,
    db=db, charset='utf8', collation='utf8_general_ci')
    cursor = mydb.cursor(dictionary=True)

    main()

    cursor.close()
    mydb.close()
