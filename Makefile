PEERS ?= 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002

.PHONY: build test test-race cluster clean

build:
	go build -o bin/kvnode ./cmd/kvnode
	go build -o bin/kvctl  ./cmd/kvctl

# Full fault-tolerance suite (election, replication, partition, restart, snapshot).
test:
	go test ./... -count=1 -timeout 300s

# Same suite under the race detector.
test-race:
	go test ./... -race -count=1 -timeout 600s

# Launch a local 3-node cluster in the background (logs in ./run/).
cluster: build
	@mkdir -p run
	@for i in 0 1 2; do \
		./bin/kvnode --id $$i --peers $(PEERS) --data run/d$$i --maxstate 4096 > run/node$$i.log 2>&1 & \
		echo "started node $$i (pid $$!)"; \
	done
	@echo "cluster up. try: ./bin/kvctl --peers $(PEERS) status"

clean:
	rm -rf bin run data data0 data1 data2
