ALGORITHM = fixed-size-64k-multithreaded
FILE = large_filler.csv
CARGO_RUN = cargo run
CARGO = cargo 
DEDUP_BUILD = "../../Oxen/target/debug/dedup"


.PHONY: test unpack pack clean clean-test

clean:
	$(CARGO) clean

test:
	$(CARGO_RUN) test --algorithm $(ALGORITHM) --input-file $(FILE)

clean-test:
	$(CARGO) clean
	$(CARGO_RUN) test --algorithm $(ALGORITHM) --input-file $(FILE)

unpack:
	$(CARGO_RUN) unpack --algorithm $(ALGORITHM) --output-file $(FILE) --input-dir 4k

pack:
	$(CARGO_RUN) pack --algorithm $(ALGORITHM) --file $(FILE) --output-dir 4k

test-oxen:
	$(DEDUP_BUILD) test-oxen --algorithm $(ALGORITHM) --input-file $(FILE)