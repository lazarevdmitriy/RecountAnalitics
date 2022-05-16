#!/bin/bash
credentials=`cat /opt/voicetech/config/mysql.conf`
user=`echo $credentials | jq '.user' | cut -d'"' -f2`
pass=`echo $credentials | jq '.pass' | cut -d'"' -f2`
base=`echo $credentials | jq '.base' | cut -d'"' -f2`
host=`echo $credentials | jq '.host' | cut -d'"' -f2`

# Install script requirements
python3 -m pip install mysql-connector elasticsearch python-dateutil

# Copy scripts
cp ./opt/voicetech/scripts /opt/voicetech/scripts

# Create index
index='phone_cdr'

curl -X PUT "localhost:9200/${index}?pretty" -H 'Content-Type: application/json' -d'
{
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
'

# Drop original function to fake
zcat recount_analytics.sql.gz | mysql -u $user --password=$pass -h $host $base

# Define cron jobs
crontab -l > /tmp/mycron
echo '0 0 * * *  /opt/voicetech/scripts/loadData.py $(date +\%Y-\%m-\%d -d "-1 day") $(date +\%Y-\%m-\%d)' >> /tmp/mycron
echo '0 1 * * *  /opt/voicetech/scripts/insertData.py $(date +\%Y-\%m-\%d -d "-3 month") $(date +\%Y-\%m-\%d)' >> /tmp/mycron
echo '' >> /tmp/mycron
crontab /tmp/mycron
rm /tmp/mycron

# Load data to elastic
/opt/voicetech/scripts/loadData.py $(date +\%Y-\%m-\%d -d "-3 month") $(date +\%Y-\%m-\%d)

# Recount analitics
/opt/voicetech/scripts/insertData.py $(date +\%Y-\%m-\%d -d "-3 month") $(date +\%Y-\%m-\%d)

