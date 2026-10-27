#!/usr/bin/env python3
"""Compatibility entry point for the final original-versus-four-bit comparison.

The old selectable dense/packed production comparison is historical. Current
preparation accepts --output and --jobs, freezes baseline6e550e1e and the working
source, and does not run benchmarks. See prepare_final.py for the manifest.
"""
from prepare_final import main

if __name__ == "__main__":
    main()
