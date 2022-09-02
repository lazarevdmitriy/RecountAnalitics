#!/usr/bin/python3
# -*- coding: utf-8 -*-
#---------------------------------------------------------------------
# Elasticsearch loading data
#
# Version      Date        Info
# 1.1          2022        Initial Version
#
# Made by Dmitriy Lazarev GOLDLINUX Copyleft (c) 2022
#---------------------------------------------------------------------
# python3 -m pip install mysql-connector elasticsearch python-dateutil

import sys, json, mysql.connector, requests
from elasticsearch import Elasticsearch
from datetime import date, datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler

delete_script = """DELETE from interaction_scripts WHERE interactionDate > '%s' and interactionDate < '%s'"""
delete_tags = """DELETE from interaction_tags WHERE interactionDate > '%s' and interactionDate < '%s'"""
delete_words = """DELETE from interaction_words WHERE interactionDate > '%s' and interactionDate < '%s'"""

get_scripts = """SELECT pcs.id, pcs.name, pcs.dataset_id, pcs.scriptType, pcst.tag_id, pcst.script_id, pcst.isManager
FROM phone_cdr_scripts pcs left join phone_cdr_script_tags pcst
on pcs.id  = pcst.script_id order by dataset_id, name"""

get_tags = """SELECT * FROM phone_cdr_tags WHERE NOT deleted order by dataset_id, name"""
get_words = """SELECT * FROM phone_cdr_tag_words WHERE tag_id = '%s'"""

add_words = """INSERT IGNORE INTO interaction_words
(interactionID,word_id,counter,interactionDate,dataset_id,isManager,word_count) VALUES ('%s', %s, %s, '%s', %s, %s, %s)"""

add_tags = """INSERT IGNORE INTO interaction_tags
(interactionID,tag_id,counter,interactionDate,dataset_id,isManager) VALUES ('%s', %s, %s, '%s', %s, %s)"""

add_scripts_soft = """ INSERT IGNORE INTO interaction_scripts ( interactionID, script_id, interactionDate, dataset_id, successful )
  SELECT interaction_tags.interactionID, phone_cdr_script_tags.script_id, interaction_tags.interactionDate, interaction_tags.dataset_id,
  (count(distinct pcsta.tag_id) = count(distinct ita.tag_id) AND count(distinct pcstc.tag_id) = count(distinct itc.tag_id))
  FROM interaction_tags
  LEFT JOIN phone_cdr_script_tags ON interaction_tags.tag_id = phone_cdr_script_tags.tag_id
  LEFT JOIN phone_cdr_scripts ON phone_cdr_scripts.id = phone_cdr_script_tags.script_id
  AND ifnull(phone_cdr_scripts.dataset_id,0) = ifnull(interaction_tags.dataset_id,0)
  LEFT JOIN phone_cdr_script_tags AS pcstc ON pcstc.script_id = phone_cdr_script_tags.script_id
  AND pcstc.isManager = 0
  LEFT JOIN phone_cdr_script_tags AS pcsta ON pcsta.script_id = phone_cdr_script_tags.script_id
  AND pcsta.isManager = 1
  LEFT JOIN interaction_tags as itc ON itc.interactionID = interaction_tags.interactionID
  AND itc.tag_id = pcstc.tag_id
  LEFT JOIN interaction_tags as ita ON ita.interactionID = interaction_tags.interactionID
  AND ita.tag_id = pcsta.tag_id
  WHERE phone_cdr_script_tags.tagType = 1
    AND phone_cdr_script_tags.tag_id IS NOT NULL
    AND phone_cdr_scripts.id IS NOT NULL
    AND phone_cdr_scripts.scriptType = "contentScript"
    AND interaction_tags.interactionDate >= '%s'
    AND interaction_tags.interactionDate <= '%s'
    GROUP BY phone_cdr_script_tags.script_id, interaction_tags.interactionID
 """

add_scripts_hard = """INSERT IGNORE INTO interaction_scripts ( interactionID, script_id, interactionDate, dataset_id, successful )
  SELECT interaction_tags.interactionID, phone_cdr_script_tags.script_id, interaction_tags.interactionDate, interaction_tags.dataset_id,
  ( group_concat(distinct pcsta.tag_id order by pcsta.id) = group_concat(distinct ita.tag_id) AND
  group_concat(distinct pcstc.tag_id order by pcstc.id) = group_concat(distinct itc.tag_id) )
  FROM interaction_tags
  LEFT JOIN phone_cdr_script_tags ON interaction_tags.tag_id = phone_cdr_script_tags.tag_id
  LEFT JOIN phone_cdr_scripts ON phone_cdr_scripts.id = phone_cdr_script_tags.script_id
  AND ifnull(phone_cdr_scripts.dataset_id,0) = ifnull(interaction_tags.dataset_id,0)
  LEFT JOIN phone_cdr_script_tags AS pcstc ON pcstc.script_id = phone_cdr_script_tags.script_id
  AND pcstc.isManager = 0
  LEFT JOIN phone_cdr_script_tags AS pcsta ON pcsta.script_id = phone_cdr_script_tags.script_id
  AND pcsta.isManager = 1
  LEFT JOIN interaction_tags as itc ON itc.interactionID = interaction_tags.interactionID
  AND itc.tag_id = pcstc.tag_id
  LEFT JOIN interaction_tags as ita ON ita.interactionID = interaction_tags.interactionID
  AND ita.tag_id = pcsta.tag_id
  WHERE phone_cdr_script_tags.tagType = 1
    AND phone_cdr_script_tags.tag_id IS NOT NULL
    AND phone_cdr_scripts.id IS NOT NULL
    AND phone_cdr_scripts.scriptType = "strictScript"
    AND interaction_tags.interactionDate >= '%s'
    AND interaction_tags.interactionDate <= '%s'
  GROUP BY phone_cdr_script_tags.script_id, interaction_tags.interactionID
  HAVING  ( group_concat(distinct pcsta.tag_id order by pcsta.id) = group_concat(distinct ita.tag_id) AND
  group_concat(distinct pcstc.tag_id order by pcstc.id) = group_concat(distinct itc.tag_id) ) IS NOT NULL
"""

load_query = """ SELECT interactionID, dataset_id, calldate as '@timestamp',
IF(dst = isManager, 'in', 'out') as direction, srcAnnotation, dstAnnotation
FROM phone_cdr pc WHERE date(calldate) BETWEEN '%s' AND '%s' AND interactionID != ''
GROUP BY interactionID;"""

def fetch(cursor, query) :
    ''' This function gets data from the database '''

    list = []
    cursor.execute(query)
    row = cursor.fetchone()
    while row is not None:
       list.append(row)
       row = cursor.fetchone()
    return list

def insert(result, cursor, isManager, tag_id, word_id):
    ''' Inserts the found results into the database '''

    for hit in result['hits']['hits']:
        interactionID = hit["_source"]['interactionID']
        interactionDate = datetime.strptime(hit["_source"]['@timestamp'], "%Y-%m-%dT%H:%M:%S")
        dataset_id = hit["_source"]['dataset_id']
        counter = 1
        word_count = 1
        # This is a fix for an issue where the index contains the wrong dataset_id value
        if dataset_id == None or dataset_id == 'None' or dataset_id == 0:
            dataset_id = 'NULL'
        logger.debug('INSERT', interactionID, dataset_id, interactionDate)
        cursor.execute(add_tags % (interactionID,tag_id,counter,interactionDate,dataset_id,isManager))
        cursor.execute(add_words % (interactionID, word_id ,counter,interactionDate,dataset_id,isManager,word_count))
        mydb.commit()

def clear_by_date(cursor, mydb, from_date, to_date):
    '''Clears the data in the database for the selected interval'''

    # cursor.execute(delete_from_phone_cdr_temp % (from_date))
    cursor.execute(delete_tags % (from_date, to_date))
    cursor.execute(delete_words % (from_date, to_date))
    cursor.execute(delete_script % (from_date, to_date))
    mydb.commit()

def load(es, index, cursor, start_load, to_date):
    '''Function to load data from mysql to elastic'''

    cursor.execute(load_query % (start_load, to_date))
    row = cursor.fetchone()
    while row is not None:
        exists = es.exists(index=index, id=row['interactionID'])
        if not exists:
            resp = es.index(index=index, id=row['interactionID'], document=row)
            logger.info(resp['result'], row['interactionID'], row['@timestamp'])
        row = cursor.fetchone()

def create_index(es, index):
    '''Create elasticsearch index'''

    mappings = {
    "settings": {
        "number_of_shards": 5
    },
    "mappings": {
        "properties": {
        "@timestamp": { "type": "date" },
        "interactionID": {"type":"keyword"},
        "dataset_id": {"type":"keyword"},
        "direction": {"type":"keyword"},
        "srcAnnotation": { "type": "text"},
        "dstAnnotation": { "type": "text"}
        }
    }
    }
    es.options(ignore_status=[400]).indices.create(
        index=index, mappings=mappings["mappings"])

def put_templates(es, templates):
    '''Create template scripts in elastic'''

    try:
        for tpl_name in templates:
            with open("templates/%s.json" % tpl_name) as f:
                tpl = json.load(f)
                es.put_script(id=tpl_name, script=tpl['script'])
    except Exception as e:
        logger.critical(e)
        sys.exit()

def clear_idx_docs_by_date(es, index, start_load):
    '''Clears the elastic index docs earlier than from_date'''
    
    try:
        query = {'range': {'@timestamp': {'lte': start_load}}}
        result = es.delete_by_query(index=index, query=query)
        query = {'range': {'@timestamp': {'lte': start_load}}}
        logger.info(f"Deleted {result['deleted']} records from index {index}")
    except Exception as e:
        logger.critical(result, e)

def elastic_search(query, sort, source):
    
    # get pit
    pit = requests.post(f'{host}/{index}/_pit?keep_alive=1m').json()
    total = es.count(index=index, query=query)["count"]
    
    # первый запрос
    result = es.search(size=size, query=query, pit=pit, sort=sort, source=source)

    if total:

        # последняя позиция
        search_after=result['hits']['hits'][-1]['sort']

        # постраничный поиск
        i=0
        while i < total:
            i+=size
            r = es.search(size=size, query=query, pit=pit, sort=sort, source=source, search_after=search_after)
            for hit in r['hits']['hits']:
                result['hits']['hits'].append( hit )
                search_after = hit['sort']

    # delete _pit
    requests.delete(f'{host}/_pit', headers={'Content-Type':'application/json'}, json={"id": pit} )

    return result

def main(cursor, es, size):
    """
    scripts(скрипты) - id.phone_cdr_scripts(имена) on script_id.phone_cdr_script_tags (данные) содержит набор маркеров
    tags(маркеры) - phone_cdr_tags содержит набор слов
     - words(слова) - phone_cdr_tag_words
    """

    scripts = fetch(cursor, get_scripts)
    tags = fetch(cursor, get_tags)

    # for all tags
    for tag in tags:
        tag_id = tag['id']
        tag_name = tag['name']
        dataset_id = tag['dataset_id']

        successful = 0; script_id = script_name = ''
        for script in scripts:
            if script['tag_id'] == tag_id:
                successful = 1
                script_id = script['id']
                script_name = script['name']
                break
        # and for every single word in single tag
        words = fetch(cursor, get_words % tag_id)
        for word in words:
            word_id = word['id']
            word_word = word['word']

            sort = {'@timestamp': {'order': 'asc', 'format': 'strict_date_optional_time_nanos'}}
            source = ["@timestamp", "interactionID", "dataset_id"]

            search_in_client_speech_dataset_default = {'bool': {'must': [], 'filter': [{'bool': {'should': [{'bool': {'filter': [{'bool': {'should': [{'match': {'direction': 'in'}}], 'minimum_should_match': 1}}, {'bool': {'should': [{'match_phrase': {'srcAnnotation': word_word}}], 'minimum_should_match': 1}}]}}, {'bool': {'filter': [{'bool': {'should': [{'match': {'direction': 'out'}}], 'minimum_should_match': 1}}, {'bool': {'should': [{'match_phrase': {'dstAnnotation': word_word}}], 'minimum_should_match': 1}}]}}], 'minimum_should_match': 1}}, {'range': {'@timestamp': {'format': 'strict_date_optional_time', 'gte': from_date, 'lte': to_date}}}], 'should': [], 'must_not': [{'exists': {'field': 'dataset_id'}}]}}

            search_in_client_speech_dataset_exists = {'bool': {'must': [], 'filter': [{'bool': {'should': [{'bool': {'filter': [{'bool': {'should': [{'match': {'direction': 'in'}}], 'minimum_should_match': 1}}, {'bool': {'should': [{'match_phrase': {'srcAnnotation': word_word}}], 'minimum_should_match': 1}}]}}, {'bool': {'filter': [{'bool': {'should': [{'match': {'direction': 'out'}}], 'minimum_should_match': 1}}, {'bool': {'should': [{'match_phrase': {'dstAnnotation': word_word}}], 'minimum_should_match': 1}}]}}], 'minimum_should_match': 1}}, {'range': {'@timestamp': {'format': 'strict_date_optional_time', 'gte': from_date, 'lte': to_date}}}, {'match_phrase': {'dataset_id': dataset_id}}], 'should': [], 'must_not': []}}

            search_in_operator_speech_dataset_default = {'bool': {'must': [], 'filter': [{'bool': {'should': [{'bool': {'filter': [{'bool': {'should': [{'match': {'direction': 'in'}}], 'minimum_should_match': 1}}, {'bool': {'should': [{'match_phrase': {'dstAnnotation': word_word}}], 'minimum_should_match': 1}}]}}, {'bool': {'filter': [{'bool': {'should': [{'match': {'direction': 'out'}}], 'minimum_should_match': 1}}, {'bool': {'should': [{'match_phrase': {'srcAnnotation': word_word}}], 'minimum_should_match': 1}}]}}], 'minimum_should_match': 1}}, {'range': {'@timestamp': {'format': 'strict_date_optional_time', 'gte': from_date, 'lte': to_date}}}], 'should': [], 'must_not': [{'exists': {'field': 'dataset_id'}}]}}

            search_in_operator_speech_dataset_exists = {'bool': {'must': [], 'filter': [{'bool': {'should': [{'bool': {'filter': [{'bool': {'should': [{'match': {'direction': 'in'}}], 'minimum_should_match': 1}}, {'bool': {'should': [{'match_phrase': {'dstAnnotation': word_word}}], 'minimum_should_match': 1}}]}}, {'bool': {'filter': [{'bool': {'should': [{'match': {'direction': 'out'}}], 'minimum_should_match': 1}}, {'bool': {'should': [{'match_phrase': {'srcAnnotation': word_word}}], 'minimum_should_match': 1}}]}}], 'minimum_should_match': 1}}, {'range': {'@timestamp': {'format': 'strict_date_optional_time', 'gte': from_date, 'lte': to_date}}}, {'match_phrase': {'dataset_id': dataset_id}}], 'should': [], 'must_not': []}}
            
            channel = tag['search_channel']; client = 0; operator = 1; no_matters = -1
            if channel == client or channel == no_matters:
                # CLIENT(0); KQL (direction : in and srcAnnotation : "оператора")  or (direction : out and dstAnnotation :"оператора")
                isManager = 0
                query = search_in_client_speech_dataset_default
                if dataset_id != None: query = search_in_client_speech_dataset_exists

            if channel == operator or channel == no_matters:
                # OPERATOR(1); KQL (direction : in and dstAnnotation : "контакт центра") or (direction : out and srcAnnotation : "контакт центра")
                isManager = 1
                query=search_in_operator_speech_dataset_default
                if dataset_id != None: query = search_in_operator_speech_dataset_exists

            result = elastic_search(query, sort, source)
            total = len(result['hits']['hits'])
            if total:
                logger.info(f"Tag::'{tag_name}' Word::'{word_word}' Match::{total}")
                insert(result, cursor, isManager, tag_id, word_id)

    # insert scripts
    cursor.execute(add_scripts_hard % (from_date, to_date))

    # phone_cdr_temp
    cursor.callproc('add_data_phone_cdr_temp')

if __name__ == '__main__':

    logger = logging.getLogger('analitics')
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler('analitics.log', maxBytes=1000000, backupCount=10)
    logger.addHandler(handler)


    try:
        with open("/opt/voicetech/config/mysql.conf") as f:
            config = json.load(f)
        with open("/opt/voicetech/config/elasticsearch.conf") as f:
            elconfig = json.load(f)
    except Exception as e:
        logger.critical('Can\'t open config')
        sys.exit()

    try:
        mydb = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            passwd=config['pass'],
            db=config['base'],
            charset='utf8',
            collation='utf8_general_ci'
        )
        cursor = mydb.cursor(dictionary=True)
        es = Elasticsearch(
            elconfig['elastic_host'], ssl_show_warn=False, verify_certs=False,
            # basic_auth=("elastic",elconfig['elastic_password']),
            # ca_certs="ca/ca.crt",
        )
        es.cluster.health()
    except Exception as e:
        logger.critical('Can\'t connect to mysql or elastic')
        sys.exit()

    if len(sys.argv) == 1:
        days_delta = datetime.now()-timedelta(days=elconfig['recount_days'])
        from_date = datetime.combine(days_delta, datetime.min.time())
        to_date = datetime.combine(date.today(), datetime.max.time())
        start_load = datetime.combine(datetime.now()-timedelta(days=elconfig['load_days']), datetime.min.time())
    elif len(sys.argv) == 3:
        from_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
        to_date = datetime.combine(datetime.strptime(sys.argv[2], "%Y-%m-%d"), datetime.max.time())
        start_load = from_date
    else:
      logger.critical("You didn't give arguments!\
        Examle: ./insertData.py '2022-06-01' '2022-07-01'")
      sys.exit()

    index  = elconfig['elastic_index']
    host  = elconfig['elastic_host']
    size = 100

    logger.info(f'Delete mysql records from {from_date} - {to_date} ...')
    clear_by_date(cursor, mydb, from_date, to_date)

    logger.info(f'Delete elastic documents from {from_date} - {to_date} ...')
    clear_idx_docs_by_date(es, index, start_load)
    create_index(es, index)

    logger.info(f'Load mysql data to elastic from {from_date} - {to_date} ...')
    load(es, index, cursor, start_load, to_date)

    logger.info(f'Recount analitics from {from_date} - {to_date} ...')
    main(cursor, es, size)

    mydb.commit()
    cursor.close()
    mydb.close()
    logger.info('OK')
    