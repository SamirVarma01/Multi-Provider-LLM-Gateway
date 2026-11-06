.PHONY: build test test-integration test-live vet up down logs smoke reconcile

build:
	go build ./...

# Integration tests start a throwaway Postgres with docker; they are skipped
# if Docker is unavailable. test-integration fails instead of skipping.
test:
	go test -race ./...

test-integration:
	REQUIRE_INTEGRATION=1 go test -race -count=1 ./...

# Live provider tests: real API calls that cost money. Skipped unless
# ANTHROPIC_API_KEY, or OPENAI_API_KEY and OPENAI_LIVE_MODEL, are set.
test-live:
	go test -tags live -run Live -count=1 -v ./internal/provider/...

vet:
	go vet ./...
	go vet -tags live ./...

up:
	docker compose up -d --build --wait

down:
	docker compose down

logs:
	docker compose logs -f gateway mock

smoke:
	./scripts/smoke.sh

# One reconciler pass from outside the gateway, for when it is down.
reconcile:
	docker compose run --rm --build reconcile
