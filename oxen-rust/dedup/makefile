ALGORITHM = fixed-size-4k
CARGO_RUN = cargo run
CARGO = cargo 


.PHONY: test unpack pack clean clean-test

clean:
	$(CARGO) clean

test:
	$(CARGO_RUN) test --algorithm $(ALGORITHM) --input-file test.parquet

clean-test:
	$(CARGO) clean
	$(CARGO_RUN) test --algorithm $(ALGORITHM) --input-file test.parquet

unpack:
	$(CARGO_RUN) unpack --algorithm $(ALGORITHM) --output-file test.parquet1 --input-dir 4k

pack:
	$(CARGO_RUN) pack --algorithm $(ALGORITHM) --file test.parquet --output-dir 4k