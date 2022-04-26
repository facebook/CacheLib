---
id: WSA_logging_library
title: A practical guide to Working Set Analysis Logging Library
---
This page describes about how you can add logging supported by Working Set Analysis Logging Library into your c++ code base.

## Prerequisite
### Create the logger config in www
Follow [this page](https://www.internalfb.com/intern/wiki/LoggerGuide/Logger_Everywhere/logger-in-c/) to create a logger config in www repo (Hack), see example D25831871.
Canonical column: https://www.internalfb.com/intern/logger/canonical_fields
### Actualize your config
Run this command in www to actualize your config (create and initialize hive table).

*After actualizing you config, there will be some restrictions to change your config*
```
phps LoggerSync Actualize YourLoggerConfig && meerkat
```
You can search your table name in Scuba to see what you have created, you will find multiple tables in your table name with different suffix, you will only find your data in your table after 24 hours, but you may see data in table ending `inc_archive` in hourly basis. For the meanings of the suffix, see [this post](https://fb.workplace.com/groups/allthelogs/permalink/5009569385758303/).
### Sync config to fbcode
```
cd fbsource/fbcode
dsi/logger/cpp/sync_logger config YourLoggerConfig
buck build dsi/logger/configs/YourLoggerConfig:logger
```
### Modify the logger config
* Add new column (non-partition): this is simple, just add a new column in the php file.
* Add partition column / change paritions: this is not allowed, you have to rename your table (new table).
* Change column type: It's not recommended to do so, it's better to give it a new name (new column).
* For more, refer to [this page](https://www.internalfb.com/intern/wiki/LoggerGuide/Quickstart_Introduction/Changing_Your_Config/)

## Overview

Since you have a logger powered by config that takes care of actualiziation (creating underlying scribe/scuba/hive tables) for you, you may wonder what is stopping you from calling logger.log() wherever you want right now. WSALoggingLib provides two functionality:

1. It supports different sampling rate based on criterium defined by you.
2. It supports customized aggregation.

In order to power these two functionality, it requires you to provide more configs to the library and your customized logic to consume those configs.

Below is how logging with WSALoggingLib works. Let's call the application `X`.

1. When the application code has an event to log, it sends it to `XWorkingSetTracker`.
2. `XWorkingSetTracker` holds `XWorkingSetSampler` and `XRequestLog`. Upon the receipt of an new event, it tests whether the event passes sampling with the sample. If it does, it forwards it to `XRequestLog`.
3. `XRequestLog` holds `XRequestRecordAggregator`. Upon the receive of the event, it creates a `XRequestRecord` based on the event. And it will be aggregated to the events with the same key.
4. `XRequestRecordAggregator` wraps around the actual logger that logs to remote service and holds a buffer of all records being aggregated in the aggregation interval. Once the aggregation interval is reached or the buffer fills up, it sends the aggregated records to the logging service (Hive) and flushes the buffer.

You will be defining all these classes whose name starts with X. But a lot of them just needs to be a definition that fills class names into already defined generic templates. And the logic required form you would mostly be related to your application's business logic.

## What classes to create

You don't have to provide all of the above implementations from scratch. In fact, a lot of time you just need to create a class by specifying class names into generic classes provided by WSALoggingLib.

This diff D21512456 contains all the classes you need to add to your code base to utilize WSALoggingLib. We'll group all these classes by their purpose and talk about each of them.

### Sampling

There are classes that tells how we do sampling. Sampling means given an event, whether we add this event into aggregation and eventually log to remote logger. Sampling is captured by WarmStorageSampler.h. The goal is that we want to create a subclass of [`Sampler`](https://fburl.com/diffusion/641ectqt) of specific types. In order to do so, we need to provide host info, telling the sampler the information of the current host (in order to select config from configerator), and a SamplerImpl, providing information about detail sampling implementation.

So below are the classes that we need to define/modify.

* [WarmStorageHostInfo](https://fburl.com/diffusion/sdcy0p6n): This is the host info class which will be used to initialize the sampler, provided by the client application.
* [WarmStorageSamplerImpl.cpp](https://fburl.com/diffusion/dv4muymd): WarmStorage samples by the blockID and host. SO we can use [KvSamplerImpl](https://fburl.com/diffusion/gistsejb) for implementation. All we need to do is to supply a static function of `graphene::ws::KvSamplerImpl::buildNewSampler`, which provides the logic to create a `KvSamplerImpl` from host information.
   * WarmStorageSamplerImpl.cpp can be renamed to anything as long as it provides KvSamplerImpl::buildNewSampler.
* [WarmStorageSampler.h](https://fburl.com/diffusion/mzx8iaav): Just to define the class `WarmStorageSampler`

Please notice that the key for the sample could be different from the record key that we would talk about later. The sampler does not store any state about the keys it sample.

#### Behind the scene

* The base class [`Sampler`](https://fburl.com/diffusion/2jipbyxj)contains logic to interact with configs changes. It supports subscription to a configerator config and handles thread safety of the rotation of config updates.
* When we call [`warmStorageSmampler.sampleRequest`](https://fburl.com/diffusion/xnk9ez5e), sampler finds the current active samplerImpl (https://fburl.com/diffusion/5m8w6iyn). Since we are calling with folly::StringPiece, KvSamplerImpl will use this [sample](https://fburl.com/diffusion/n0w3nvny) function, which uses furcHash on the string key.

### Record

[WarmStorageRequestRecord.h](https://fburl.com/diffusion/i8vfeamp) defines the record class. Every WSALoggingLib record class is a `graphene::ws::RequestRecord of` certain key type. [WarmStorageRequestKey](https://fburl.com/diffusion/eavvpgkp) is the unit of aggregation, which means within the update interval, all the records with the same key will be merged together. For warm storage, every field of the record is part of the key. So when merge happens, only the [opCount](https://fburl.com/diffusion/xtr4al7m) field of the base RequestRecord base class will be incremented.

In another example, [ZippyDBRecord](https://fburl.com/diffusion/b4hd1bv6) has field that are merged in its own `merge` function.

### Aggregator

To user the aggregation, we need to define the following classes:

* [WarmStorageRequestRecordAggregator](https://fburl.com/diffusion/pjabms42) = `RequestRecordAggregator<WarmStorageRequestRecord>`. This simply defines an interface class that consumes the record class.
* [WarmStorageRequestRecordAggregatorHive](https://fburl.com/diffusion/0csjpg6r): This is the implementation of the aggregator that finally logs the aggregated records to Hive. This class also populates host information into the hive record.
* [WarmStorageRequestLog](https://fburl.com/diffusion/n4q9ujt2) = `RequestLog<WarmStorageRequestRecord, folly::SpinLock>`

That's it. There is very little myth to implement since all the complicated processing are hidden in the base class below.

#### Behind the scene

* The base class [RequestLog](https://fburl.com/diffusion/07mu6cx8) contains the core logic for aggregation. This class stores data in a [number](https://fburl.com/diffusion/j29slucd) of [shards](https://www.internalfb.com/intern/diffusion/FBS/browse/master/fbcode/graphene/working_set/common/RequestLog.h?commit=6ee0e67a92c7&lines=499). Each shard contains a map from keys to records. The reason for sharding is to reduce locking contention.
* Each shard has its own memory arena to store variable sized keys (strings).
* A thread runs an [aggregation loop](https://fburl.com/diffusion/pm6nzwze) that collects shards that can be aggregated and log to remote logging and flush the memory.
* When the a new event comes, [recordOperation](https://fburl.com/diffusion/uzdp0gdk) gets called and it is where [record merge](https://fburl.com/diffusion/k7kfx8kt) happens.

### API

The lass class we need to define is the interface class where the application holds a reference and call into.

The class [`WarmStorageWorkingSetTracker`](https://fburl.com/diffusion/iyo9v0wk) contains the WarmStorageSampler and WarmStorageRequestLog. It provides one customized method [logOP](https://fburl.com/diffusion/avxrl328) that can be called by the application to send an event. We initialize this class with some parameters that it feeds into either the sample or request log:

* nShards: Number of shards for the RequestLog. This number does not affect the overall size since the below max configs are for overall not per shard.
* collectionInterval: Default collection time interval. Records will be flushed at least once per this time interval.
* maxBacklog: Largest number of aggregated records we keep before flushing.
* maxBufferSize: The size of buffer (arena size) that stores the variable sized keys. This is roughly the memory "overhead" by WSALoggingLib.

Example of logging in application: https://fburl.com/diffusion/k3rsv2m0

### Unit tests

Please make sure you write unit tests! Examples: https://fburl.com/diffusion/qnb6p95n

You can create mock aggregator to avoid logging to hive and to examine what you actually logged.

### Canary Test
To test your code in production data, you need to setup a canary testing environement, build your pkg and deploy it to the canary machine.
The following steps/examples are for WarmStorage, other applications may be different.
1. Ask a WarmStorage engineer (or @haowux) to set up a canary test cluster. You will get a cluster name, e.g. `ws.sandbox.denny07`
2. Build the package. Make sure you have cloned configerator repo then run
`fbpkg build -E warm_storage.storage_service --expire 28d`
When you finished this sucessfully, you can see a finished progress bar like `============` and under that you can find your package `warm_storage.storage_service:xxxxxx`

You will be prompt for permission for the first time, request for two months. Then run the above command again it will succeed.
You should see the link to request permission in error message, but if not, use [this link](https://www.internalfb.com/intern/hipster/request/create/?acl_type=FBPKG&acl_name=WARMSTORAGE_FBPKG).

Run `buck clean` before `fbpkg` to avoid running out of space in your dev server.

3. Canary. We are canarying to `denny07`:[link](https://www.internalfb.com/intern/tupperware/details/job/?handle=priv2_cln%2Fwarm_storage%2Fws.sandbox.denny07.storage_service). Choose one task/host from the cluster, lets say we are using task 1, find the host name from above link, e.g. `warmstorage114.04.cln2`

4. Canary the configerator config. Make your configerator change on the sampling config by adding sampling rate 1 to cluster `denny07` (rate 1 means chossing all). e.g. D25934246
```
arc build
hg commit
arc canary --hosts warmstorage114.04.cln2
```

5. now canary to ws
```
sf canary --tw-job priv2_cln/warm_storage/ws.sandbox.denny07.storage_service --tasks 1 --duration 1d --sfid warm_storage/storage_service --num-control-tasks 0 -V warm_storage.storage_service:xxxxxx --canary-task-extra-args='--sfn_cachelib_enable_ml_admission --sfn_cachelib_store_features --sfn_cachelib_ml_admission_metadata_override="conveyor_ash8strontium_20201111" --sfn_cachelib_ml_admission_target_recall_override=0.7'
```

You will be prompt for permission for the first time, request for two months. Then run the above command again it will succeed.
You should see the link to request permission in error message, but if not, use [this link](https://www.internalfb.com/intern/hipster/request/create/?acl_type=TIER&acl_name=ws-acl.customer.sandbox)


## What configs to create

There are two configs in configerator that needs to be supplied:

* Sampling config definition: D21534544
* Adding specific sampling config for your hosts: D21695295
   * This config can be picked up without restarting hosts if you set up the sampler [by subscription](https://fburl.com/diffusion/5rhxyzpl).
   * You may even define a host level config to test out your sampling by canarying this configerator change to one single host.
