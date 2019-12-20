set /p dir="Enter directory for import: "

docker volume rm datanode1
docker volume rm datanode2
docker volume rm datanode3
docker volume rm namenode
docker volume rm zookeeper
docker volume rm zookeeper_log

docker volume create datanode1
docker volume create datanode2
docker volume create datanode3
docker volume create namenode
docker volume create zookeeper
docker volume create zookeeper_log

docker run --rm --ip 172.200.0.1 --hostname importer1 --name importer1 -v datanode1:/hadoop/dfs/data -t -d debian:9
docker run --rm --ip 172.200.0.2 --hostname importer2 --name importer2 -v datanode2:/hadoop/dfs/data -t -d debian:9
docker run --rm --ip 172.200.0.3 --hostname importer3 --name importer3 -v datanode3:/hadoop/dfs/data -t -d debian:9
docker run --rm --ip 172.200.0.4 --hostname importer4 --name importer4 -v namenode:/hadoop/dfs/name -t -d debian:9
docker run --rm --ip 172.200.0.5 --hostname importer5 --name importer5 -v zookeeper:/data -v zookeeper_log:/datalog -t -d debian:9

docker cp %dir%/datanode1/data importer1:hadoop/dfs/
docker cp %dir%/datanode2/data importer2:hadoop/dfs/
docker cp %dir%/datanode3/data importer3:hadoop/dfs/
docker cp %dir%/namenode/name importer4:hadoop/dfs/
docker cp %dir%/zooData/data importer5:/
docker cp %dir%/zooDatalog/datalog importer5:/

docker stop importer1
docker stop importer2
docker stop importer3
docker stop importer4
docker stop importer5
