cd /mnt/myria/myriadeploy 
./kill_all_java_processes.py  deployment.cfg.ec2 
./setup_cluster.py deployment.cfg.ec2
./launch_cluster.sh deployment.cfg.ec2