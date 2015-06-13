if [ $# -ne 2 ]; then
	echo 'deploy.sh [workers_per_node] [redundancy]'
    exit 1
fi

WORKERS_PER_NODE=$1
REDUNDANCY=$2

nodelist=`grep "server-." /opt/scidb/14.12/etc/config.ini | cut -f 2 -d "=" | cut -f 1 -d "," | sort | uniq`
nodes=${nodelist/$'\n'/ }
echo "$nodelist"
echo "$nodelist" > /root/nodes.txt

/opt/scidb/14.12/bin/scidb.py stopall mydb

#deployment/deploy.sh prepare_toolchain master
#deployment/deploy.sh prepare_coordinator master
#su scidb -c "./run.py setup -f"

su scidb -c "./run.py make -j4"
su scidb -c "./run.py make_packages /tmp/packages -f"

while read node; do
  scp -r /mnt/scidb/stage/build/debian/scidb-14.12-plugins/mnt/scidb/stage/install/* $node:/opt/scidb/14.12
done < /root/nodes.txt

deployment/deploy.sh scidb_install /tmp/packages $nodes

/opt/scidb/14.12/bin/scidb.py stopall mydb

while read node; do
  scp -r /mnt/scidb/stage/install/* $node:/opt/scidb/14.12
done < /root/nodes.txt

/opt/scidb/14.12/bin/scidb.py stopall mydb

deployment/deploy.sh scidb_prepare scidb "scidb" mydb mydb mydb /mnt/scidb/db $WORKERS_PER_NODE default $REDUNDANCY $nodes

/opt/scidb/14.12/bin/scidb.py initall mydb -f
/opt/scidb/14.12/bin/scidb.py startall mydb
