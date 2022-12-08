---
id: understanding_nvm_latency
title: Understanding Nvm Latency
---

In this doc, we describe how to interpret the latency numbers in NVM operations.

There are two types of latency stats exported to ODS.
- Overall latency: `nvm.lookup.latency_us`, `nvm.insert.latency_us`. This latency counts everything related starting from the calling of nvmCache_->find/put to scheduling, reading/writing from device, and callbacks.
- Device latency: `nvm.navy_device_write_latency_us`, `nvm.navy_device_read_latency_us`. This latency counts only the device operations (for example, `:pread`, `:pwrite`).

If the two latency is similar, it means the NVM operations are bottlenecked by the device. If the overall latency is not acceptable, you can consider standard approaches to reduce device latencies. This typically means reduce the number of IOs to the device. An example would be checking the alignment size to see if we are running more IOs than we should.

If device latency is lower than overall latency by a large factor, it probably means a lot of time is spent in scheduling. We can then look at navy worker pool stats (take readers for example).
- All stats can be found at `regex(.*reader_pool.*)`
- `jobs_done.60` is the number of jobs completed in the past 60 seconds.
- `reschedules.60` is the number of retries in the past 60 seconds.
- `high_reschedule` is the number of jobs completed that has been rescheduled more than 250 times since the process started.
- `max_queue_len` is the maximum size of queue in the past 60 seconds. (We have a number of queues configured by `navyConfig.setReaderAndWriterThreads`)
- `pending_jobs` is the sum of `max_queue_len` across all queues.

Below are some of the common situations:
- If `max_queue_len` is large, it means you are likely going to have a large p95+ because it takes a long wait time in one of the queues.
- If `pending_jobs~=max_queue_len*num_reader_threads`, it means all queues are equally busy, which is most likely the case because tasks are assigned by round-robin. In this case, increasing the number of readers would help.
- If `reschedules.60` is high, it means there are lots of retries. You can then look into the possible reasons for retries, such as concurrent inserts.
