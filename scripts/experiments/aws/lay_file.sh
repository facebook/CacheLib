diskFileSizeGB=$1
flashFileSizeGB=$2

# two different devices so could be done together 
dd if=/dev/urandom of=/disk/disk.file bs=1M count=$((${diskFileSizeGB}*1024)) oflag=direct &
dd if=/dev/urandom of=/flash/cache.file bs=1M count=$((${flashFileSizeGB}*1024)) oflag=direct &
