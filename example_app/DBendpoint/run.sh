#docker run --name myjob -v myvoljob:/opt/assets --network=example_app_HelloNetwork -e ITER=101 myjob:latest
docker system prune -f && 
docker run --name myjob --cpus 1 --label example_app_jar --memory 512M --network=example_app_HelloNetwork -e ITER=$1 myjob:latest