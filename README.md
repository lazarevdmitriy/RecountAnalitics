# Recount Analitics
Project for bank to search marker words in elasticsearch

# Install elastic search

## On host
Run install_elastic.sh for Centos7
## On Docker
Download image `docker pull docker.elastic.co/elasticsearch/elasticsearch:7.17.1`
and run `docker run -p 127.0.0.1:9200:9200 -p 127.0.0.1:9300:9300 -e "discovery.type=single-node" -d --restart unless-stopped docker.elastic.co/elasticsearch/elasticsearch:7.17.1`

# Then install scripts, create indexes and load data ....
Run install.sh

### On the system that has no access to internet
Download packeges `pip3 download mysql-connector elasticsearch python-dateutil`
Then install it on the remote host `pip3 install --no-index --find-links /path/to/download/dir/ mysql-connector elasticsearch python-dateutil`

