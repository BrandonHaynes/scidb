while read node; do
  ssh $node -n 'rm /tmp/*'
  ssh $node -n 'sudo -u postgres psql -c "drop database if exists myria" && sudo -u postgres psql -c "create database myria" && sudo -u postgres psql -c "grant all privileges on database myria to uwdb;"'
done < /root/workers.txt
