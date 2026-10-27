#!/usr/bin/env python3
"""Analytical allocation metadata sizes; no timing or Python object sizes.

Counts describe configured extents, not only allocated extents. Hash costs
assume full-identity 16-byte slots and target load, before allocator/lock/resize
costs. Free-index sizes are additional to whatever tracks ownership.
"""
import argparse
import json
import math
from decimal import Decimal, InvalidOperation
from fractions import Fraction


def decimal_fraction(value):
    try:
        parsed = Decimal(value)
        if not parsed.is_finite():
            raise ValueError("nonfinite value")
        return Fraction(parsed)
    except (InvalidOperation, ValueError, OverflowError) as error:
        raise argparse.ArgumentTypeError("expected a finite decimal number") from error


def sizes(extents, escaped_fraction, byte_escaped_fraction, slot_bytes, load):
    escaped = math.ceil(extents * escaped_fraction)
    byte_escaped = math.ceil(extents * byte_escaped_fraction)
    bitmap_words = (extents + 63) // 64
    hierarchy_bytes = 0
    words = bitmap_words
    while words > 1:
        words = (words + 63) // 64
        hierarchy_bytes += words * 8
    return {
        "configured_extents": extents,
        "escaped_extents_raw_ge_3": escaped,
        "byte_escaped_extents_raw_ge_255": byte_escaped,
        "dense32_bytes": extents * 4,
        "packed2_base_bytes": (extents + 3) // 4,
        "packed2_plus_hash_bytes": (extents + 3) // 4
        + math.ceil(escaped / load) * slot_bytes,
        "byte_base_bytes": extents,
        "byte_plus_hash_bytes": extents
        + math.ceil(byte_escaped / load) * slot_bytes,
        "free_bitmap_bytes": bitmap_words * 8,
        "free_bitmap_summary_bytes": hierarchy_bytes,
        "notes": "Packed base rounded to bytes, not uint64 words; no hash power-of-two rounding, locks, resize peak, or durable copies",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capacity-tib", type=decimal_fraction, default=Fraction(1))
    parser.add_argument("--extent-kib", type=int, default=128)
    parser.add_argument("--escape-fraction", type=decimal_fraction, default=Fraction(1, 100),
                        help="fraction of configured extents with raw count >=3")
    parser.add_argument("--byte-escape-fraction", type=decimal_fraction, default=Fraction(0),
                        help="fraction of configured extents with raw count >=255")
    parser.add_argument("--slot-bytes", type=int, default=16)
    parser.add_argument("--load", type=decimal_fraction, default=Fraction(7, 10))
    args = parser.parse_args()
    if not 0 < args.capacity_tib * (1 << 40) <= (1 << 64) - 1:
        parser.error("capacity must be positive and fit in a uint64 byte address space")
    if args.extent_kib <= 0 or args.slot_bytes <= 0:
        parser.error("extent and slot sizes must be positive")
    if not 0 < args.load < 1:
        parser.error("load must lie strictly between 0 and 1")
    if not 0 <= args.byte_escape_fraction <= args.escape_fraction <= 1:
        parser.error("require 0 <= byte escape fraction <= escape fraction <= 1")
    extents = int(args.capacity_tib * (1 << 40)) // (args.extent_kib * 1024)
    if extents == 0:
        parser.error("capacity must contain at least one extent")
    result = sizes(extents, args.escape_fraction, args.byte_escape_fraction,
                   args.slot_bytes, args.load)
    result["packed2_asymptotic_break_even_vs_dense32_fraction"] = float(Fraction(15, 4) * args.load / args.slot_bytes)
    result["packed2_asymptotic_break_even_vs_byte_no_escape_fraction"] = float(Fraction(3, 4) * args.load / args.slot_bytes)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
