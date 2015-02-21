while read node; do
  ssh $node 'rm /tmp/*'
  ssh $node 'sudo -u postgres psql -c "drop database if exists myria" && sudo -u postgres psql -c "create database myria" && sudo -u postgres psql -c "grant all privileges on database myria to uwdb;"'
done < /root/workers.txt
