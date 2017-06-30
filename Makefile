docker-build:
	TAG=$$(date +%Y%m%d%H%M%S) ;\
	docker pull debian:stretch ; \
	docker build -t idgo/idgo_extract-app:$$TAG . ; \
	docker build -t idgo/idgo_extract-app:latest . ; \
	docker build -t idgo/idgo_extract-worker:$$TAG celery ; \
	docker build -t idgo/idgo_extract-worker:latest celery ; \

docker-stop-rm:
	docker-compose stop
	docker-compose rm -f

docker-clean-volumes:
	docker-compose down --volumes --remove-orphans

docker-clean-images:
	docker-compose down --rmi 'all' --remove-orphans

docker-clean-all:
	docker-compose down --volumes --rmi 'all' --remove-orphans

all: docker-build
