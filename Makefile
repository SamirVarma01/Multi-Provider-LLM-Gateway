.PHONY: build test test-integration vet up down logs smoke

build:
	go build ./...

# Integration tests start a throwaway Postgres with docker; they are skipped
# if Docker is unavailable. test-integration fails instead of skipping.
test:
	go test -race ./...

test-integration:
	REQUIRE_INTEGRATION=1 go test -race -count=1 ./...

vet:
	go vet ./...

up:
	docker compose up -d --build --wait

down:
	docker compose down

logs:
	docker compose logs -f gateway mock

smoke:
	./scripts/smoke.sh
