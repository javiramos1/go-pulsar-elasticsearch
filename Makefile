TAG = 0.0.1


db:
	docker-compose up
stop:
	docker-compose down

build:
	docker build -t go-pulsar-elasticsearch:$(TAG) . 

run:
	docker run go-pulsar-elasticsearch:$(TAG)

push:
	docker push go-pulsar-elasticsearch:$(TAG)
