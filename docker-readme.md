Building the containers
=======================
Requirements:
Docker is a must, however docker-compose is optional.

Either run one of the following commands in the root of the repo:

1. make all
or
2. docker-compose build

To access the app after it is done building:

docker exec -it splice-app /bin/bash

Once in the container:

1. cd spliceengine/
2. ./start-splice-cluster
