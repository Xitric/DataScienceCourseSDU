set /p dir="Enter export location: "

docker build --rm -f "importer\Dockerfile" -t importer:latest "importer"

docker run --rm --ip 172.200.0.1 --hostname exporter1 --name exporter1 -v datanode1 importer:latest
docker run --rm --ip 172.200.0.2 --hostname exporter2 --name exporter2 -v datanode2 importer:latest
docker run --rm --ip 172.200.0.3 --hostname exporter3 --name exporter3 -v datanode3 importer:latest
docker run --rm --ip 172.200.0.4 --hostname exporter4 --name exporter4 -v zookeeper -v zookeeper_log importer:latest

docker cp exporter1:/hadoop/dfs/data %dir%/datanode1/
docker cp exporter2:/hadoop/dfs/data %dir%/datanode2/
docker cp exporter3:/hadoop/dfs/data %dir%/datanode3/
docker cp exporter4:/data %dir%/zoo/
docker cp exporter4:/datalog %dir%/datalog/

docker stop exporter1
docker stop exporter2
docker stop exporter3
docker stop exporter4
