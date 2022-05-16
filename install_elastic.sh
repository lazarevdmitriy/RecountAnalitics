#!/bin/bash

version=7

# repo
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root"
   exit 1
fi

rpm --import https://artifacts.elastic.co/GPG-KEY-elasticsearch
cat >/etc/yum.repos.d/elasticsearch.repo <<EOL
[elasticsearch-$version.x]
name=Elasticsearch repository for $version.x packages
baseurl=https://artifacts.elastic.co/packages/$version.x/yum
gpgcheck=1
gpgkey=https://artifacts.elastic.co/GPG-KEY-elasticsearch
enabled=1
autorefresh=1
type=rpm-md
EOL

# selinux
echo 0 > /sys/fs/selinux/enforce
sed -i 's/SELINUX=enforcing/SELINUX=disabled/g' /etc/selinux/config
sed -i 's/SELINUX=permissive/SELINUX=disabled/g' /etc/selinux/config

# RPM'S
yum update -y
yum install java-1.8.0-openjdk elasticsearch  kibana python3 -y
python3 -m pip install mysql-connector elasticsearch python-dateutil

# Configuring services
ip=$(hostname -I | awk '{print $1}')
ram_gb=$(free -g | awk '/^Mem:/{print $2}')
ram=$(( ${ram_gb} / 2 ))
if [ ${ram} -eq "0" ]; then
		ram=1;
fi

sed -i "s/#elasticsearch.hosts: .*/elasticsearch.hosts: [\"http:\/\/127.0.0.1:9200\"]/g" /etc/kibana/kibana.yml
sed -i "s/#server.host: .*/server.host: $ip/g" /etc/kibana/kibana.yml

sed -i "s/-Xms1g/-Xms${ram}g/" /etc/elasticsearch/jvm.options
sed -i "s/-Xmx1g/-Xmx${ram}g/" /etc/elasticsearch/jvm.options

# Creating security rules
systemctl start firewalld
systemctl enable firewalld
firewall-cmd --permanent --zone=public --add-port=5601/tcp
firewall-cmd --reload

# Starting services
systemctl daemon-reload
systemctl enable elasticsearch kibana
systemctl start elasticsearch kibana
echo "Use \"admin\" as login and \"password\" as password for login to http://$ip:5601"
