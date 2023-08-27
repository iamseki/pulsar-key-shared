consumer:
	go run cmd/consumer/main.go

producer:
	go run cmd/producer/main.go

pulsar:
	docker compose up