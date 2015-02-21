nodelist=$(cat /root/nodes.txt)
nodes=${nodelist/$'\n'/ }

/opt/scidb/14.8/bin/scidb.py stopall mydb

deployment/deploy.sh scidb_prepare scidb "scidb" mydb mydb mydb /mnt/scidb/db 1 default 1 $nodes

/opt/scidb/14.8/bin/scidb.py initall mydb -f
/opt/scidb/14.8/bin/scidb.py startall mydb
