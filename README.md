# CS739-Device-Failure-prediction

### Setup python3 virtual environment using:

```
virtualenv -p python3 env
. env/bin/activate
```
Always remember to activate the environment, before installing any dependencies or starting webserver.

## Setup data paths using:
```
make setup
```

### To run using docker
```
1. To build and run the container for the first time: docker-compose up -d --build (-d option will detach the process from the session, --build is required only if changes are made to docker files)
2. To stop the docker container: docker-compose down
3. To know status of each container: docker ps
4. To check logs: docker logs
5. Access specific container: docker exec -i -t container_name/container_id /bin/bash
```

### Not using docker:
```bash
make # to install dependencies
make run # to run webserver 
```

### Swagger document available at:
```
http://<base_url>/ui
```