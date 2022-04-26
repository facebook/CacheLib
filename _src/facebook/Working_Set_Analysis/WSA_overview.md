---
id: WSA_overview
title: Working Set Analysis Overview
---

### POCs
- Nick Stanisha (Infrastructure Data Science)
- Pietari Pulkkinen (Infrastructure Data Science)
- Sathya Gunasekar (Cachelib)
- Jimmy Lu (Cachelib)
- Eden Zik (Memcache)
- Zifan Yang (Memcache)
- Marc Light (Manager, Infrastructure Data Science)

### Links
- [Working Set Analysis UI](https://www.internalfb.com/cachelib/wsa/)
- [Workplace group](https://fb.workplace.com/groups/452627415509944)

### Overview
Working Set Analysis (WSA) is a collaborative project between CacheLib, Memcache, and Infra Data Science which aims to provide tools for analyzing and understanding cache workload patterns to improve cache performance. WSA itself is an umbrella term which encompasses a variety of offline analyses & models. At its core, the WSA project is an **offline traffic analysis framework** with an easy configurator-based opt-in for cache owners who are interested in optimizing their caches.

Working Set Analysis is not a cache simulation project. The insights derived from WSA are insights about the traffic that a cache must serve. WSA aims to help you optimize your cache to meet the demands of your traffic.

For information about caches, working sets, "traffic", etc. please see [Helpful Definitions](WSA_helpful_definitions).

### Analysis
More information on the following analyses can be found in the "Analysis and Optimizations" section.
- Working set monitoring (including set intersection and overlap)
- Hit rate vs. cache size estimates for LRU-like caches
- Object churn and future value models, which encompasses
  - Tuning time-to-access evictions
  - Flash admission models
- Traffic partitioning analyses e.g. Arena assignment and sizing for TAO

### UI
The [Working Set Analysis UI is a self-service](https://www.internalfb.com/cachelib/wsa) way to monitor your use case(s). We currently show metrics for [our multi-tenant cache analysis](https://www.internalfb.com/intern/wiki/Cache_Library_User_Guides/Analyses_and_Optimizations/#hit-rate-analyses-guaran). Features include:
- Filters for Date, Granularity, Server and separate regex filters for each granularity selected that control everything on the page
- A chart for Cache Size vs Hit Rate
- An accompanying table with additional information. Users can change the "Lifetime in seconds" option to view the data for a given lifetime (e.g. 60 seconds) in the table
- To make the relationship between data in the table and the chart clear, hovering over a row in the table highlights the corresponding point on the chart (also based off the selected "Lifetime in seconds")

You can also go to the UI easily via bunnylol `wsa {myusecase}`
