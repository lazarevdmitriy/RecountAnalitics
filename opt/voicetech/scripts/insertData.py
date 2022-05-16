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
# python3 -m pip install mysql-connector elasticsearch python-dateutil

import sys, datetime, json, mysql.connector, requests
from elasticsearch import Elasticsearch
from dateutil import parser

delete_script = ("DELETE from interaction_scripts "
    "WHERE interactionDate > '%s' and interactionDate < '%s'")
delete_tags = ("DELETE from interaction_tags "
    "WHERE interactionDate > '%s' and interactionDate < '%s'")
delete_words = ("DELETE from interaction_words "
    "WHERE interactionDate > '%s' and interactionDate < '%s'")

get_scripts = """ SELECT pcs.id, pcs.name, pcs.dataset_id, pcs.scriptType, pcst.tag_id, pcst.script_id, pcst.isManager
    FROM phone_cdr_scripts pcs left join phone_cdr_script_tags pcst  on pcs.id  = pcst.script_id   """

get_tags = """ SELECT * FROM phone_cdr_tags WHERE NOT deleted """
get_words = """ SELECT * FROM phone_cdr_tag_words WHERE tag_id = '%s' """

add_words = ("INSERT IGNORE INTO interaction_words "
"(interactionID,word_id,counter,interactionDate,dataset_id,isManager,word_count) "
"VALUES ('{}', {}, {}, '{}', {}, {}, {})")

add_tags = ("INSERT IGNORE INTO interaction_tags "
"(interactionID,tag_id,counter,interactionDate,dataset_id,isManager) "
"VALUES ('{}', {}, {}, '{}', {}, {})")

add_scripts_soft = """   INSERT IGNORE INTO interaction_scripts ( interactionID, script_id, interactionDate, dataset_id, successful )
  SELECT interaction_tags.interactionID,
         phone_cdr_script_tags.script_id,
         interaction_tags.interactionDate,
         interaction_tags.dataset_id,
		 ( count(distinct pcsta.tag_id) = count(distinct ita.tag_id) AND count(distinct pcstc.tag_id) = count(distinct itc.tag_id) )
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

add_scripts_hard = """  INSERT IGNORE INTO interaction_scripts ( interactionID, script_id, interactionDate, dataset_id, successful )
  SELECT interaction_tags.interactionID,
         phone_cdr_script_tags.script_id,
         interaction_tags.interactionDate,
         interaction_tags.dataset_id,
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

# search
elastic_host="http://localhost:9200"
index = "phone_cdr"
time_zone = "Europe/Moscow"
size='100'
sort = [{"@timestamp":{"order":"asc","format":"strict_date_optional_time_nanos"}}]
source = ["@timestamp", "interactionID"]

def fetch(query) :
    list = []
    mycursor.execute(query)
    row = mycursor.fetchone()
    while row is not None:
       list.append(row)
       row = mycursor.fetchone()
    return list

def elastic_search(query):

    # get pit
    pit = requests.post(f'{elastic_host}/{index}/_pit?keep_alive=1m').json()

    # первый запрос
    result = es.search(size=size, query=query, pit=pit, sort=sort)
    total = result['hits']['total']['value']

    if total:
        # последняя позиция
        search_after=result['hits']['hits'][-1]['sort']

        # постраничный поиск
        i=1
        while i < total:
            i+=1
            r = es.search(size=size, query=query, pit=pit, sort=sort, search_after=search_after)
            for hit in r['hits']['hits']:
                result['hits']['hits'].append( hit )
                search_after = hit['sort']

    # delete _pit
    requests.delete(f'{elastic_host}/_pit', headers={'Content-Type':'application/json'}, json={"id": pit} )

    return result

def insert(result, isManager, successful, script_id, tag_id, word_id):

    if result['hits']['total']['value']:
        for hit in result['hits']['hits']:
            interactionID = hit["_source"]['interactionID']
            interactionDate = parser.parse( hit["_source"]['@timestamp'])
            dataset_id = hit["_source"]['dataset_id']
            counter = 1
            word_count = 1

            if dataset_id == None or dataset_id == 'None' or dataset_id == 0:
                dataset_id = 'NULL'

            # insert tags
            mycursor.execute(add_tags.format(interactionID,tag_id,counter,interactionDate,dataset_id,isManager))
            # insert words
            mycursor.execute(add_words.format(interactionID, word_id ,counter,interactionDate,dataset_id,isManager,word_count))
            mydb.commit()

def clear_by_date(start, stop):
    mycursor.execute(delete_tags % (start, stop))
    mycursor.execute(delete_words % (start, stop))
    mycursor.execute(delete_script % (start, stop))
    mydb.commit()

def main():
    """
    скрипты - id.phone_cdr_scripts(имена) on script_id.phone_cdr_script_tags (данные)
    маркеры - phone_cdr_tags
    слова - phone_cdr_tag_words
    """

    clear_by_date(start, stop)
    scripts = fetch(get_scripts)
    # {'id': 13, 'name': 'Упоминание каналов самообслуживания', 'dataset_id': 70, 'scriptType': 'strictScript', 'tag_id': 155, 'script_id': 13, 'isManager': 1}
    tags = fetch(get_tags)
    # {'id': 1, 'name': 'WOW card', 'deleted': 1, 'emotion': 4, 'class_id': 1, 'dataset_id': 70, 'tagType': 'standardTag', 'search_channel': -1}

    for tag in tags:
        tag_id = tag['id']

        # проверка принадлежит ли тег скрипту
        successful = 0; script_id = script_name = ''
        for script in scripts:
            if tag['id'] == script['tag_id']:
                successful = 1
                script_id = script['id']
                script_name = script['name']

        for word in fetch(get_words % tag_id):
            # [{'id': 8889, 'tag_id': 266, 'word': 'Один'}]
            word_id = word['id']
            word_word = word['word']

            range  = { "@timestamp": { "format": "strict_date_optional_time", "gte": start, "lte": stop, "time_zone": time_zone } }
            client_query = {
            "bool": {
              "must": [],
              "filter": [
                {
                  "bool": {
                    "should": [
                      {
                        "bool": {
                          "filter": [
                            {
                              "bool": {
                                "should": [
                                  {
                                    "match": {
                                      "direction": "in"
                                    }
                                  }
                                ],
                                "minimum_should_match": 1
                              }
                            },
                            {
                              "bool": {
                                "should": [
                                  {
                                    "match_phrase": {
                                      "srcAnnotation": word_word
                                    }
                                  }
                                ],
                                "minimum_should_match": 1
                              }
                            }
                          ]
                        }
                      },
                      {
                        "bool": {
                          "filter": [
                            {
                              "bool": {
                                "should": [
                                  {
                                    "match": {
                                      "direction": "out"
                                    }
                                  }
                                ],
                                "minimum_should_match": 1
                              }
                            },
                            {
                              "bool": {
                                "should": [
                                  {
                                    "match_phrase": {
                                      "dstAnnotation": word_word
                                    }
                                  }
                                ],
                                "minimum_should_match": 1
                              }
                            }
                          ]
                        }
                      }
                    ],
                    "minimum_should_match": 1
                  }
                },
                {
                  "range": range
                }
              ],
              "should": [],
              "must_not": []
            }
          }
            operator_query = {
            "bool": {
              "must": [],
              "filter": [
                {
                  "bool": {
                    "should": [
                      {
                        "bool": {
                          "filter": [
                            {
                              "bool": {
                                "should": [
                                  {
                                    "match": {
                                      "direction": "in"
                                    }
                                  }
                                ],
                                "minimum_should_match": 1
                              }
                            },
                            {
                              "bool": {
                                "should": [
                                  {
                                    "match_phrase": {
                                      "dstAnnotation": word_word
                                    }
                                  }
                                ],
                                "minimum_should_match": 1
                              }
                            }
                          ]
                        }
                      },
                      {
                        "bool": {
                          "filter": [
                            {
                              "bool": {
                                "should": [
                                  {
                                    "match": {
                                      "direction": "out"
                                    }
                                  }
                                ],
                                "minimum_should_match": 1
                              }
                            },
                            {
                              "bool": {
                                "should": [
                                  {
                                    "match_phrase": {
                                      "srcAnnotation": word_word
                                    }
                                  }
                                ],
                                "minimum_should_match": 1
                              }
                            }
                          ]
                        }
                      }
                    ],
                    "minimum_should_match": 1
                  }
                },
                {
                  "range": range
                }
              ],
              "should": [],
              "must_not": []
            }
          }

            channel = tag['search_channel']; client = 0; operator = 1; no_matters = -1
            if channel == client or channel == no_matters:
                # CLIENT(0); KQL (direction : in and srcAnnotation : "оператора")  or (direction : out and dstAnnotation :"оператора")
                isManager = 0
                result = elastic_search(client_query)
                insert(result, isManager, successful, script_id, tag_id, word_id)
                print(f"tag: {tag['name']}; word: {word_word}; found: {result['hits']['total']['value']}; script: {script_name}")

            if channel == operator or channel == no_matters:
                # OPERATOR(1); KQL (direction : in and dstAnnotation : "контакт центра") or (direction : out and srcAnnotation : "контакт центра")
                isManager = 1
                result = elastic_search(operator_query)
                insert(result, isManager, successful, script_id, tag_id, word_id)
                print(f"tag: {tag['name']}; word: {word['word']}; found: {result['hits']['total']['value']}; script: {script_name}")

    # insert scripts
    mycursor.execute(add_scripts_hard % (start, stop))
    # mycursor.execute(add_scripts_soft % (start, stop))
    # phone_cdr_temp
    mycursor.callproc('add_data_phone_cdr_temp')
    mydb.commit()

    # close connection
    mycursor.close()
    mydb.close()

if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 3:
      print("\nYou didn't give arguments!\n"
      "Examle: ./insertData.py '2022-02-24' '2022-02-25'")
      sys.exit(0)
    elif len(sys.argv) == 2:
        from_date = date_to = sys.argv[1]
    elif len(sys.argv) == 3:
        _, from_date, date_to = sys.argv

    try:
        start = datetime.datetime.strptime(f"{from_date}T00:00:00.000Z","%Y-%m-%dT%H:%M:%S.%fZ")
        stop = datetime.datetime.strptime(f"{date_to}T23:59:59.000Z","%Y-%m-%dT%H:%M:%S.%fZ")

        with open("/opt/voicetech/config/mysql.conf") as f:
            config = json.load(f)
            host=config['host']
            db=config['base']
            user=config['user']
            passwd=config['pass']
    except Exception as e:
        print('Script init error')
        sys.exit(0)

    mydb = mysql.connector.connect(host=host, user=user, passwd=passwd,
    db=db, charset='utf8', collation='utf8_general_ci')
    mycursor = mydb.cursor(dictionary=True)
    es = Elasticsearch(elastic_host)

    main()
