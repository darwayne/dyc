SHELL=/bin/bash -o pipefail

project_module=github.com/darwayne/dyc
dynamo_test_end_point="http://localhost:8000"


up:
	docker-compose up -d

# run normal unit and integration tests
test: test-unit test-integration

# run unit tests only.
test-unit:
	go test -v -tags="unit" -race ./...

# run integration tests only. these tests expect various dependencies to be up and running
test-integration: up
	@DYNAMO_TEST_ENDPOINT=$(dynamo_test_end_point) go test -v -tags="integration" -race ./...

unit-test-tags: ## Updates all unit tests so they have appropriate build tags (only if they don't already have a build tag)
	@for f in `find . -type f -name "*_test.go" | grep -v "integration_test.go"` ; do \
		noBuildFlags=$$(head -n 1 $$f | grep -v "//+build") ; \
		if [[ $$noBuildFlags ]] ; then \
		    echo "added missing unit build tag to: $$f"; \
			result=$$((echo -e "//+build unit\n"); cat $$f); \
			echo "$$result" > $$f; \
		fi ; \
	done ; \

int-test-tags: ## Updates all integration tests so they have appropriate build tags (only if they don't already have a build tag)
	@for f in `find . -type f -name "*_test.go" | grep "integration_test.go"` ; do \
		noBuildFlags=$$(head -n 1 $$f | grep -v "//+build") ; \
		if [[ $$noBuildFlags ]] ; then \
		    echo "added missing integration build tag to: $$f"; \
			result=$$((echo -e "//+build integration\n"); cat $$f); \
			echo "$$result" > $$f; \
		fi ; \
	done ; \

missing-test-tags: unit-test-tags int-test-tags


# golint requires golint install via:
# GO111MODULE="off" go get -u golang.org/x/lint/golint
golint:
	golint ./...
# importorder requires impi. to install run the following outside of the repo:
# GO111MODULE="off" go get github.com/pavius/impi/cmd/impi
#
importorder: ## Verifies all code has correct import orders (stdlib, 3rd party, internal)
	@STATUS=0 ; \
	for f in `find . -type d` ; do \
		file=$$(cd $$f && impi --local $(project_module) --scheme stdThirdPartyLocal --ignore-generated=true .) ; \
		if [[ $$file ]] ; then \
		echo "$$f/$$file" ; \
			STATUS=1 ; \
		fi ; \
	done ; \
	if [ $$STATUS -ne 0 ] ; then \
		exit 1 ; \
	fi

fmt: ## Verifies all code is gofmt'ed
	@STATUS=0 ; \
	for f in `find . -type f -name "*.go"` ; do \
		file=$$(gofmt -l $$f) ; \
		if [[ $$file ]] ; then \
			echo "file not gofmt'ed: $$f" ; \
			STATUS=1 ; \
		fi ; \
	done ; \
	if [ $$STATUS -ne 0 ] ; then \
		exit 1 ; \
	fi

vet:
	go vet ./...

# staticcheck requires staticcheck. to install run the following outside of the repo:
# GO111MODULE="off" go get honnef.co/go/tools/cmd/staticcheck
#
staticcheck: ## runs staticcheck on our packages
	staticcheck $(project_module)/...

# GO111MODULE="off" go get github.com/fzipp/gocyclo
cyclo:
	@gocyclo -over 15 .

# verify code is up to basic standards
verify: golint staticcheck vet fmt importorder

touch-all-files:
	find . -type f -name "*.go" -exec touch {} +
