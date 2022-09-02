#!/bin/bashs
# Define cron jobs
crontab -l | egrep -v 'recountanalitics|^$' > /tmp/mycron
echo '@daily  /srv/recountanalitics/insertData.py' >> /tmp/mycron
echo '' >> /tmp/mycron
crontab /tmp/mycron
rm /tmp/mycron
crontab -l