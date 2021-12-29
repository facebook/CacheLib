CACHELIB_DIR=$1
sudo CACHELIB_DIR/contrib/build.sh -j -d 
cd CACHELIB_DIR/cachelib/external/fmt 
git checkout 90034e4c4bafa7467885bc60cbf8a0465fb848b5
cd CACHELIB_DIR
sudo CACHELIB_DIR/contrib/build.sh -j -d -S 
