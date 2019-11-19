echo Shutting down HBase gracefully...
docker exec -it hbase hbase master stop
timeout 20
docker-compose -f "docker-compose.yml" down