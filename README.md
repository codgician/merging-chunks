# Merging chunks

First time using Rust to build something, might need lots of improvements (eager for suggestions).

## Problem

Given n sorted `int64` **chunks** (arrays) meeting following conditions:

- Values inside each chunk satisfy **uniform distribution**.
- Size of all chunks is approximately **1TiB**.

The task here is to design and implement an alogrithm that merges all sorted chunks into one single sorted chunk. It is expected to take advantage of multi-core processor and perform well with no more than 16GiB of RAM.

## Main idea

- Split `[0 .. 2^64 - 1]` into 512 (2^9) *partitions* with equal length. Denote i-th part as `p[i]`, then `p[i] = [i * 2^55 .. i * 2^56  - 1]`;
- For each *partition* `p[i]`:
  -  Iterate over all chunks, take all values that ranges within `p[i]` and store them in array `a[i]`;
  -  The length of each array `a[i]` will be unlikely to exceed memory limit as values are distributed uniformly;
  -  Sort array `a[i]` parallelly (taking advantage of multi-core processor), and write back to disk.
- Concatenate all result files on disk.

## Other possible improvements

- If values **do not** satisfy uniform distribution, we could use **binary search** algorithm to determine the bounds of each partition (to make them close in length). This costs O(logn) more times of I/O read for each value.  

## Testing & Performance Analysis

**WIP**

Due to hardware limitations, size of all chunks are limited to **256MiB**, and we expect our algorithm to get everything done using no more than approximately **1MiB** of memory (which maintains the x256 ratio).
