set /p dir="Enter export location: "

docker run --rm --ip 172.200.0.1 --hostname exporter1 --name exporter1 -v datanode1:/hadoop/dfs/data -t -d debian:9
docker run --rm --ip 172.200.0.2 --hostname exporter2 --name exporter2 -v datanode2:/hadoop/dfs/data -t -d debian:9
docker run --rm --ip 172.200.0.3 --hostname exporter3 --name exporter3 -v datanode3:/hadoop/dfs/data -t -d debian:9
docker run --rm --ip 172.200.0.4 --hostname exporter4 --name exporter4 -v namenode:/hadoop/dfs/name -t -d debian:9
docker run --rm --ip 172.200.0.5 --hostname exporter5 --name exporter5 -v zookeeper:/data -v zookeeper_log:/datalog -t -d debian:9
docker run --rm --ip 172.200.0.6 --hostname exporter6 --name exporter6 -v mysql:/var/lib/mysql -t -d debian:9

docker cp exporter1:/hadoop/dfs/data %dir%/datanode1/
docker cp exporter2:/hadoop/dfs/data %dir%/datanode2/
docker cp exporter3:/hadoop/dfs/data %dir%/datanode3/
docker cp exporter4:/hadoop/dfs/name %dir%/namenode/
docker cp exporter5:/data %dir%/zooData/
docker cp exporter5:/datalog %dir%/zooDatalog/
docker cp exporter6:/var/lib/mysql %dir%/sfdb/

docker stop exporter1
docker stop exporter2
docker stop exporter3
docker stop exporter4
docker stop exporter5
docker stop exporter6
