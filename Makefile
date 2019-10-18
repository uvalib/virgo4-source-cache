GOCMD = go
GOBUILD = $(GOCMD) build
GOCLEAN = $(GOCMD) clean
GOTEST = $(GOCMD) test
GOGET = $(GOCMD) get
GOMOD = $(GOCMD) mod
GOFMT = $(GOCMD) fmt
GOVET = $(GOCMD) vet
PACKAGENAME = virgo4-source-cache
BINNAME = $(PACKAGENAME)

build: darwin 

all: darwin linux

darwin:
	GOOS=darwin GOARCH=amd64 $(GOBUILD) -a -o bin/$(BINNAME).darwin cmd/$(PACKAGENAME)/*.go

linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -a -installsuffix cgo -o bin/$(BINNAME).linux cmd/$(PACKAGENAME)/*.go

clean:
	$(GOCLEAN) cmd/
	rm -rf bin

dep:
	cd cmd/$(PACKAGENAME); $(GOGET) -u
	$(GOMOD) tidy
	$(GOMOD) verify

fmt:
	cd cmd/$(PACKAGENAME); $(GOFMT)

vet:
	cd cmd/$(PACKAGENAME); $(GOVET)
