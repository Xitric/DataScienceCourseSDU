set /p dir="Enter directory for import: "

docker build --rm -f "importer\Dockerfile" -t importer:latest "importer"

docker run --rm --ip 172.200.0.1 --hostname importer1 --name importer1 -v datanode1 importer:latest
docker run --rm --ip 172.200.0.2 --hostname importer2 --name importer2 -v datanode2 importer:latest
docker run --rm --ip 172.200.0.3 --hostname importer3 --name importer3 -v datanode3 importer:latest
docker run --rm --ip 172.200.0.4 --hostname importer4 --name importer4 -v zookeeper -v zookeeper_log importer:latest

docker cp %dir%/datanode1/ importer1:/hadoop/dfs/data
docker cp %dir%/datanode2/ importer2:/hadoop/dfs/data
docker cp %dir%/datanode3/ importer3:/hadoop/dfs/data
docker cp %dir%/zoo/ importer4:/data
docker cp %dir%/datalog/ importer4:/datalog

docker stop importer1
docker stop importer2
docker stop importer3
docker stop importer4
