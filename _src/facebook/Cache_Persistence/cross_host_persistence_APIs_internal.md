---
id: Cross_Host_Persistence_APIs_Internal
title: Cross Host Persistence APIs (internal)
---

## Introduction
Cachelib cross host persistence feature requires user to provide customized stream reader and writer to communicate with backend remote storage. [Manifold](https://www.internalfb.com/intern/wiki/Manifold/Manifold_Overview/) is widely used inside FB as remote storage, and it is most likely to be the first choice of FB users, so Cachelib has provided built-in Manifold reader and writer.

## How To Use Manifold Reader And Writer
[Reader](https://www.internalfb.com/diff/D29218346):
```cpp
PersistenceManager manager(cacheConfig);
ManifoldStreamReader reader(
      "bucket name",
      "object path",
      std::chrono::seconds{5000},               // timeout
      std::thread::hardware_concurrency() - 1,  //parallelDownloads
      "",                                       //apiKey
      "",                                       //localPath
      500 * 1024 * 1024                         //bufferSize 500MB
);
manager.restoreCache(reader);
```

[Writer](https://www.internalfb.com/diff/D29218345):
```cpp
PersistenceManager manager(cacheConfig);
ManifoldStreamWriter writer(
     "bucket name",
     "directory path",
     "filename",
     true,                                    //userData
     std::chrono::seconds{5000},              //timeout
     std::chrono::seconds{50000},             //ttl
     std::thread::hardware_concurrency() - 1, //parallelUploads
     "",                                      //apiKey
     "",                                      //localPath
     500 * 1024 * 1024,                       //bufferSize 500MB
 );
manager.saveCache(writer);
writer.close();
```
[Example](https://www.internalfb.com/diff/D28423748).


## Ture The Performance
**Buffer Mode** in reader downloads part of data to memory from manifold for each time.

**Buffer mode** in writer uploads data to a separate temporary object in manifold, and concat all temp objects to the target at the end. The temporary objects have fixed 1 day expiry time, so the persistence must finish within 1 day.

This is the default by giving empty localPath in reader/writer constructor, tune **bufferSize** to get better performance.

**File mode** in reader downloads the whole file to a local file from manifold at the beginning, and reads a part from the file into memory each time.

**File mode** in writer persists data to a local file, and upload the file to manifold at the end.

Provide a **localPath** to enable this mode. The bufferSize doesn’t play a big role in performance but helps in memory usage. User need to ensure no other thread/process is mutating the given file.

To get better performance, user can compare File mode and Buffer mode with various buffer size. Based on previous testing, Buffer mode with large buffer size gives the best performance, while File mode perform better than a small buffer in Buffer mode.

`parallelDownloads` / `parallelUploads` are the number of thread Manifold client will use for uploading/downloading objects, more threads gives better performance. Reader and writer will download or upload objects asynchronously in buffer mode, so reader will prepare next data block while the current one is still using by PersistenceManager; writer will return immediately (except flushing) to PersistenceManager and uploads in background.
