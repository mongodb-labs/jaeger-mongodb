GIT_HASH?=$(shell git log --pretty=format:'%h' -n 1)
DOCKER_NAMESPACE?=quay.io/jaeger-mongodb

BASE_IMAGE?=alpine:3.14.0
JAEGER_VERSION?=1.23.0

.PHONY: test
test:
	go vet ./...
	go test ./...

.PHONY: clean
clean::
	rm jaeger-mongodb

.PHONY: build-linux
build-linux: clean
	GOOS=linux go build ./cmd/jaeger-mongodb

.PHONY: docker-build
docker-build: build-linux
	for component in collector query ; do \
  		docker build . -f ./cmd/jaeger-mongodb/Dockerfile.$$component \
  			--build-arg base_image=$(BASE_IMAGE) \
  			--build-arg jaeger_version=$(JAEGER_VERSION) \
  			-t $(DOCKER_NAMESPACE)/jaeger-$$component-mongodb:$(JAEGER_VERSION)-$(GIT_HASH) ; \
  	done

.PHONY: docker-push
docker-push: docker-build
	for component in collector query ; do \
  		docker push $(DOCKER_NAMESPACE)/jaeger-$$component-mongodb:$(JAEGER_VERSION)-$(GIT_HASH) ; \
  	done
