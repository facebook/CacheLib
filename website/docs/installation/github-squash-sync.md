---
title: Github Repository Synchronization (after cleanup)
id: github-squash-sync
---

In preparation of making **CacheLib** public, the CacheLib github repository
has a new branch `main` (instead of `master`), and all previous commits
have been squashed (merged into one).

If you cloned/forked the **CacheLib** repository **after September 2021**, there
is no need to run the commands below - your repository is up-to-date.

When trying to update an existing cloned CacheLib repository (e.g. on
your local work computer), The following error might appear:

    $ git pull
    fatal: refusing to merge unrelated histories

Use the following instructions to synchronize your local repository with CacheLib's.
NOTES about the commands;
1. This assumes you have no local changes
2. This assumes the "origin" remote is `git@github.com:facebook/CacheLib.git` .

First, fetch all the remote updates:

    $ git fetch --all
    Fetching origin
    remote: Enumerating objects: 174, done.
    remote: Counting objects: 100% (174/174), done.
    remote: Compressing objects: 100% (75/75), done.
    remote: Total 174 (delta 54), reused 174 (delta 54), pack-reused 0
    Receiving objects: 100% (174/174), 398.69 KiB | 1.28 MiB/s, done.
    Resolving deltas: 100% (54/54), completed with 5 local objects.
    From github.com:facebook/CacheLib
    + eb0a8b64...45eb4ad3 main       -> origin/main  (forced update) #### This indicates the branch was squashed
    d12f6273..3d6e81b8  gh-pages   -> origin/gh-pages

Second, checkout the updated remote 'main' branch, and overwrite any
existing local 'main' branches (`-B`)

    $ git checkout -B main -t origin/main
    Branch 'main' set up to track remote branch 'main' from 'origin'.
    Reset branch 'main'


Your branch is up to date with 'origin/main'.

Third, verify you have the updated branch, with only few commits (as
of Aug 13, 2021):

    $ git log --pretty=oneline
    45eb4ad36b2b94b223319ee11326c3bb9a560bf5 (HEAD -> main, origin/main) Updating submodules
    a0cf7942cae3f83e60fe3bca5b2ffc87fb1572fe Initial commit


## Advanced topic: updated a forked repository on github

If you created a forked repository on Github, it is very likely that
the `origin` git remote points to YOUR forked repository, not to the
`facebook/CacheLib` repository.

In such cases it is customary to locally add a second git-remote
source and name it `upstream`, so that `git pull` from `origin` gets
your private changes (and similarly, `git push` to `origin` uploads
you changes to GitHub), and `git pull` from `upstream` gets the latest
changes from the CacheLib team.

First step, check which remote sources are defined in your local repository:

    $ git remote show
    origin

    $ git remote show origin | grep Fetch
    Fetch URL: git@github.com:agordon/CacheLib.git

In the above example, there is only one remote source (named `origin`)
and it points to a personal (*agordon*'s) forked repository.

To add a second remote source, run the following command:

    $ git remote add upstream git@github.com:facebook/CacheLib.git
    $ git remote show
    origin
    upstream

Synchronize all remote sources:

    $ git fetch --all
    Fetching origin
    Fetching upstream
    From github.com:facebook/CacheLib
    * [new branch]        gh-pages                         -> upstream/gh-pages
    * [new branch]        main                             -> upstream/main

Checkout the updated `main` branch from the upstream source, overwriting the
existing local `main` branch (if it exists):

    $ git checkout -B main upstream/main
    Reset branch 'main'
    Your branch is up to date with 'upstream/main'.

Now update (`git push --force`) your github fork, with the updated "main" branch content:

    $ git push --force --set-upstream origin main:main
    Total 0 (delta 0), reused 0 (delta 0)
    remote:
    remote: Create a pull request for 'main' on GitHub by visiting:
    remote:      https://github.com/agordon/CacheLib/pull/new/main
    remote:
    To github.com:agordon/CacheLib.git
    * [new branch]        main -> main

You can now delete the old "master" branch from your forked repository.

The following command deletes a remote branch. If it fails with the following error:

    $ git push --delete origin master
    To github.com:agordon/CacheLib.git
    ! [remote rejected]   master (refusing to delete the current branch: refs/heads/master)
    error: failed to push some refs to 'git@github.com:agordon/CacheLib.git'

This means that on your local github forked repository, you did not
yet change the default branch from "master" to "main" (as the CacheLib
repository did on August 12, 2021).

Go to the "settings" page of your forked CacheLib github repository
(i.e. `https://github.com/USERNAME/CacheLib/settings/branches`),
select "branches" (from the left-side menu bar), then change the
"Default Branch" settings from "master" to "main".

Now try the command again:

    $ git push --delete myfork master
    To github.com:agordon/CacheLib.git
    - [deleted]           master

You can also delete the local "master" branch, as it is no longer needed:

    $ git branch -D master
    Deleted branch master (was a19b4c0a).
