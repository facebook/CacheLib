---
id: WSA_overview
title: Working Set Analysis Overview
---

### POCs
- Jimmy Lu (CacheLib)
- Hao Wu (CacheLib)
- Pranav Bhandari (CacheLib)
- Vivek Jain (Core Data Manager)

### Overview
WSA (Working Set Analysis) is an offline traffic analysis framework with an easy configurator-based opt-in tracing framework and local scripts that analyze workloads to debug, and find optimization opportunities. Our goal is to empower users to independently monitor, optimize and troubleshoot their caches.

### Analysis
This is a list of analysis that WSA supports and plans to support
- Hit Rate: Generate hit rate curve locally. [Done]
- ROI: Compute the ROI of current or prospective cache workloads. [Current]
- Reinsertion using Guarenteed Lifetime: Given the hit rate of different types in the workload, compute the benefits of reinsertion based on guarenteed lifetime for each type. [Current]
- Allocation class: Generate the popularity and hit rate of each allocation class. This information can be used to estimate idea allocation class sizes for a workload. 
