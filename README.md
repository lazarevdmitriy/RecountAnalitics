# Recount Analitics
Project for bank to search marker words in elasticsearch

# Quick start
Run elasic `docker-compose up -d`
Install requirements `pip3 install -r requirements.txt`
Run script `python3 insertData.py`

### On the system that has no access to internet
Download packeges `pip3 download -r requirements.txt`
Then install it on the remote host `pip3 install --no-index --find-links /path/to/download/dir/ -r requirements.txt`
