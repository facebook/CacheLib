diskDir=/disk 
flashDir=/flash
diskFilePath=${diskDir}/disk.file
cacheFilePath=${flashDir}/cache.file 
fmtCorrectionCommit=90034e4c4bafa7467885bc60cbf8a0465fb848b5
cacheLibURL=https://github.com/pbhandar2/CacheLib
cacheLibDir=/home/ubuntu/CacheLib 
diskFilePath=/dev/xvdb
flashFilePath=/dev/nvme1n1 
diskFileSizeGB=300
flashFileSizeGB=300

./mkfs.sh $diskFilePath $flashFilePath $diskDir $flashDir
./lay_file.sh $diskFileSizeGB $flashFileSizeGB

cd /home/ubuntu 
git clone $cacheLibURL
./install_cachelib.sh $cacheLibDir

