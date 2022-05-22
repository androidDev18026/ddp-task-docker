#docker run --name myjob -v myvoljob:/opt/assets --network=example_app_HelloNetwork -e ITER=101 myjob:latest
docker system prune -f && 
docker run --name myjob --cpus 1 --label example_app_jar --memory 512M --network=example_app_HelloNetwork -e ITER_REDIS=$1 -e ITER_IGNITE=$2 myjob:latest