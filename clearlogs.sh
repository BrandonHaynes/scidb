while read node; do
  ssh -n $node truncate --size 0 /mnt/scidb/db/*/*/scidb.log
done < /root/workers.txt