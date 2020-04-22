# Merging chunks

![](https://github.com/codgician/merging-chunks/workflows/merging-chunks/badge.svg) | [简体中文](README_CN.md)

Using Rust to build something for the first time, might need lots of improvements (eager for suggestions).

## Problem

For convenience, the implementation here processes `unsigned int64` rather than `int64`.

Given `k` sorted `uint64` **chunks** (arrays) meeting following conditions:

- Values inside each chunk satisfy **uniform distribution**.
- The size of all chunks is approximately **1TiB**.

The task here is to design and implement an algorithm that merges all sorted chunks into one single sorted chunk. It is expected to take advantage of multi-core processors and perform well with no more than **16GiB** of RAM.

In this implementation, `k` chunks are stored in disk files named `0.in`, `1.in` ... `(k - 1).in`. To make observing results easier, values are stored as strings and separated with SPACE. The program will produce a result file named `result.txt`.

## Main idea

It is impossible to load and process everything in RAM, making Disk I/O a highly possible bottleneck when it comes to actual performance. Therefore, it is necessary to make trade-offs between complexity and I/O frequency. Since the codomain of the values is relatively limited (0 ~ 2^64 - 1), so my solution is to split the codomain into different partitions, process each partition separately and finally merge them. 

- Split `[0 .. 2^64 - 1]` into 512 (2^9) *partitions* with equal length. Denote i-th part as `p[i]`, then `p[i] = [i * 2^55 .. i * 2^56  - 1]`;
- For each *partition* `p[i]`:
  -  Iterate over all chunks, filter out values that range within `p[i]` and store them in array `a[i]`;
  -  The length of each array `a[i]` will be unlikely to exceed memory limit as values are distributed uniformly;
  -  Sort array `a[i]` using parallel sorting algorithm, and append them to the end of the result file.

## Optimizations

### Implemented

- In early commits, after analyzing using `perf`, I found that the bottleneck lies in frequent disk I/O:
  - Use `BufReader` to read chunks in a buffered way, which reduces the number of system calls and improves performance;
  - Stop immediately if a value larger than the upper bound of current *partition* is read (this value will be cached in RAM). This guarantee that every value will only be read O(1) times. 
- Another equivalent way to implement the filtering step is to first create a file for each *partition*, then iterate over all *chunks*, for every value we figure out which *partition* it should be in and we append it to the end of the corresponding file.
  - **Pros**: Compared to the algorithm above, for partitions, we no longer need to cache values into RAM. Therefore, it could handle scenarios where the number of chunks is relatively huge.
  - **Cons**: When the data set is relatively small, this algorithm introduces lots of random disk writes. However, since values satisfy uniform distribution, when the data set is huge, multiple consecutive values from the same *chunk* highly likely belongs to the same *partition*. With `BufWriter`, a buffered way to write, it might ease the loss of performance.
  - **To be implemented**: When the data set is relatively small, we use the algorithm discussed before, otherwise use the algorithm discussed here.

### Furthermore...

- What if values **DOES NOT** satisfy uniform distribution?
  - **Algorithm #1**: Following the idea above, we could use binary search to determine bounds of each *partition*. However, it will introduce O(klogn) more I/O operations for each value, which might significantly impact actual performance.
  - **Algorithm #2**: Take the idea behind merging two sorted arrays using the two-pointer algorithm:
    - **Steps**: 
      - Select a value for `s`, representing *block size*;
      - Maintain a min-heap of size `k * s` inside RAM;
      - For each *chunk*, take out first **s** values and insert them into the min-heap;
      - Maintain an array `num`, where `num[i]` represents the number of values held inside the min-heap. As for now, for each i, `num[i] = s`;
      - Keep popping the top element of the min-heap and write it into the result file. Denoting i as the index of the partition where this element comes from, at the same time we check of `num[i] == 0`. If so, we insert the next s values from this partition into the min-heap;
      - Keep going until the min-heap becomes empty.
    - **Analysis**: 
      - The idea behind *block size* `s` is to avoid highly fragmented random readings. However, if the number of chunks is huge, **s** has to be small (could be resolved using divide and conquer approach which costs another O(logn) in complexity);
      - It's hard to boost it using multi-threading (might involve complex lockings).