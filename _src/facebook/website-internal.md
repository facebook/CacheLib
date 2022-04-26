---
title: Website Updates - FB Internals
id: website-internal
---

## Internal Static Docs

At https://www.internalfb.com/intern/staticdocs/cachelib/ .

## Configuration


## Deployment

```sh
yarn build
GITHUB_USER=<your user> yarn deploy
```

## Local Testing

```sh
$ FB_INTERNAL=1 yarn run start --host $(hostname -f) --port 8085
```
