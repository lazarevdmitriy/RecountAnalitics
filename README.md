# Recount Analitics
Project for bank to search marker words in elasticsearch

# Quick start
- Check docker status `docker ps` and ensure what we don't have another elastic applications  on same ports
- Run elasic `docker-compose up -d`
- Copy config `cp templates/elasticsearch.conf /opt/voicetech/config`
- Check variables recount_days and load_days `mcedit /opt/voicetech/config/elasticsearch.conf`
- Install requirements `pip3 install -r requirements.txt`
- Run script `python3 insertData.py`
- If you needs edit crontab `@daily cd /root/gold/recountanalitics && python3 insertData.py`

### On the system that has no access to internet
Download packeges `pip3 download -r requirements.txt`
Then install it on the remote host `pip3 install --no-index --find-links /path/to/download/dir/ -r requirements.txt`
