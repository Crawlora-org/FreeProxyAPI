.PHONY: test race vet fmt fmt-check check kustomize

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

# fmt rewrites files in place; fmt-check only reports (used by check and CI).
fmt:
	gofmt -l -w .

fmt-check:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "gofmt needed on:"; echo "$$unformatted"; exit 1; fi

kustomize:
	kubectl kustomize k8s/base > /dev/null

check: fmt-check vet test race
