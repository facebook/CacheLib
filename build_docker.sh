sudo -E docker build --build-arg http_proxy=$http_proxy --build-arg https_proxy=$https_proxy -t cachelib:dto --build-arg GETDEPS_PATCH_B64="$(base64 -w0 patches/getdeps-ubuntu24.patch)" .
