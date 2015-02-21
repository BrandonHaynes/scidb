nodelist=$(cat /root/nodes.txt)
nodes=${nodelist/$'\n'/ }

/opt/scidb/14.8/bin/scidb.py stopall mydb

#deployment/deploy.sh prepare_toolchain master
#deployment/deploy.sh prepare_coordinator master
#su scidb -c "./run.py setup -f"

su scidb -c "./run.py make -j4"
su scidb -c "./run.py make_packages /tmp/packages -f"

while read node; do
  scp -r /mnt/scidb/stage/build/debian/scidb-14.8-plugins/mnt/scidb/stage/install/* $node:/opt/scidb/14.8
done < /root/nodes.txt

deployment/deploy.sh scidb_install /tmp/packages $nodes

/opt/scidb/14.8/bin/scidb.py stopall mydb

while read node; do
  scp -r /mnt/scidb/stage/install/* $node:/opt/scidb/14.8
done < /root/nodes.txt

/opt/scidb/14.8/bin/scidb.py stopall mydb

deployment/deploy.sh scidb_prepare scidb "scidb" mydb mydb mydb /mnt/scidb/db 1 default 1 $nodes

/opt/scidb/14.8/bin/scidb.py initall mydb -f
/opt/scidb/14.8/bin/scidb.py startall mydb
