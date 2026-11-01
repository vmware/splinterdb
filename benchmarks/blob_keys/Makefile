# Invoke from the repository root:
# make -f Makefile -f benchmarks/blob_keys/Makefile blob-key-bench
.PHONY: blob-key-bench
blob-key-bench: $(BINDIR)/blob_key_bench
$(BINDIR)/blob_key_bench: $(OBJDIR)/benchmarks/blob_keys/blob_key_bench.o $(OBJDIR)/benchmarks/blob_keys/blob_key.o $(OBJDIR)/benchmarks/blob_keys/blob_key_checks.o $(LIBDIR)/libsplinterdb.a
-include $(OBJDIR)/benchmarks/blob_keys/blob_key_bench.d $(OBJDIR)/benchmarks/blob_keys/blob_key.d $(OBJDIR)/benchmarks/blob_keys/blob_key_checks.d
