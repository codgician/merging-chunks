# Merging chunks

![](https://github.com/codgician/merging-chunks/workflows/Rust/badge.svg)

Using Rust to build something for the first time, might need lots of improvements (eager for suggestions).

## Problem

For convenience, the implementation here processes `unsigned int64` rather than `int64`.

Given **k** sorted `uint64` **chunks** (arrays) meeting following conditions:

- Values inside each chunk satisfy **uniform distribution**.
- The size of all chunks is approximately **1TiB**.

The task here is to design and implement an algorithm that merges all sorted chunks into one single sorted chunk. It is expected to take advantage of multi-core processors and perform well with no more than **16GiB** of RAM.

## Main idea

It is impossible to load and process everything in RAM, making Disk I/O a highly possible bottleneck when it comes to actual performance. Therefore, it is necessary to make trade-offs between complexity and I/O frequency. Since the codomain of the values is relatively limited (0 ~ 2^64 - 1), so my solution is to split the codomain into different partitions, process each partition separately and finally merge them. 

- Split `[0 .. 2^64 - 1]` into 512 (2^9) *partitions* with equal length. Denote i-th part as `p[i]`, then `p[i] = [i * 2^55 .. i * 2^56  - 1]`;
- For each *partition* `p[i]`:
  -  Iterate over all chunks, take all values that range within `p[i]` and store them in array `a[i]`;
  -  The length of each array `a[i]` will be unlikely to exceed memory limit as values are distributed uniformly;
  -  Sort array `a[i]` parallelly (taking advantage of multi-core processors), and write back to disk.
- Concatenate all result files on disk.

