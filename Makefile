.PHONY: base-java base-mvn base-protobuf splice-app all


splice-app:
	docker build -t splice-app:latest -f splice_machine_docker/splice_app/Dockerfile .

base-java:
	docker build -t base-java:latest -f splice_machine_docker/base_java/Dockerfile .

base-protobuf:
	docker build -t base-java:latest -f splice_machine_docker/base_protobuf/Dockerfile .

base-mvn:
	docker build -t base-mvn:latest -f splice_machine_docker/base_mvn/Dockerfile .

all: base-java base-mvn base-protobuf splice-app
