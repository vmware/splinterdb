---
title: "Introducing SplinterDB"
slug: introducing-splinterdb
date: 2022-05-19
author: Carlos Garcia-Alvarado
image: /img/blogs/blog-placeholder.png
excerpt: "It is our pleasure to be introducing SplinterDB, an embedded high-performance general-purpose key-value store built from the ground up to maximize the utilization of the latest generation of hardware (e.g., NVMe storage drives)."
tags: ['Carlos Garcia-Alvarado']
---

It is our pleasure to be introducing SplinterDB, an embedded high-performance general-purpose key-value store built from the ground up to maximize the utilization of the latest generation of hardware (e.g., NVMe storage drives).

SplinterDB's journey started in 2017 when one of our internal products had difficulty finding a key-value store that could satisfy its speed requirements to scale to a larger volume of metadata operations. Our research and product teams embraced the challenge and invented a novel data structure, the STB epsilon tree, that could meet its high-performance requirements. But what makes SplinterDB different from other key-value stores? First, it represents a data structure breakthrough that is theoretically optimal in its performance and avoids the tradeoff that key-value stores have to make between accelerating reads or writes. In other words, with SplinterDB, there is no need to compromise. It is excellent at both! Second, we designed SplinterDB to be small, portable,  embeddable in multiple platforms, and CPU and memory-efficient. Third, SplinterDB was architected to be an innovation platform in the space of storage engines, which could incorporate the latest advances by the research community.

Our benchmarks, including the Yahoo Cloud Services Benchmark (YCSB), were published in [USENIX ATC 2020]( https://www.usenix.org/conference/atc20/presentation/conway),  show that SplinterDB outperforms leading alternatives like RocksDB in multiple dimensions. In particular, SplinterDB surpassed RocksDB by up to seven times in insertions, four times in reads, in device bandwidth utilization, and on non-random workloads performance. Moreover, SplinterDB showed to scale with the number of cores and memory. 

In March 2022, the SplinterDB library became open source. This milestone is the culmination of our pursuit of making SplinterDB an open storage engine that is welcoming to innovation. Similarly, our open storage engine prevents vendor lock-in. But the journey of SplinterDB is just getting started, and you can still be part of this journey! We invite you to be part of our community, and if you can think of an application that could benefit from SplinterDB, join us in our mission.
