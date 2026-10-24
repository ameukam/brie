GO ?= go

.PHONY: build fmt fmt-check vet test e2e conformance-smoke conformance fuzz lint vuln release-check check

build:
	$(GO) build -trimpath -o bin/brie-node ./cmd/brie-node

fmt:
	$(GO) fmt ./...

fmt-check:
	@files="$$(gofmt -l .)" || exit $$?; \
	if [ -n "$$files" ]; then \
		printf 'Run make fmt on:\n%s\n' "$$files"; \
		exit 1; \
	fi

vet:
	$(GO) vet ./...

test:
	$(GO) test -race -coverprofile=coverage.out ./...
	@$(GO) tool cover -func=coverage.out | awk \
		'/^total:/ { seen = 1; print; if ($$3 + 0 <= 80) { print "Coverage must exceed 80%"; exit 1 } } \
		END { if (!seen) exit 1 }'

fuzz:
	$(GO) test ./cmd/brie-node -run='^$$' -fuzz='^FuzzRun$$' -fuzztime=10s -parallel=2

e2e:
	$(GO) test -tags=e2e -count=1 -timeout=3m -v ./tests/e2e

conformance-smoke:
	bash tests/e2e/conformance.sh quick

conformance:
	bash tests/e2e/conformance.sh certified-conformance

lint:
	golangci-lint run

vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...

release-check:
	goreleaser check

check: fmt-check vet test fuzz lint vuln release-check
