flash=$1
disk=$2

diskDir=/disk
flashDir=/flash 

sudo mkfs -t ext4 $flash 
sudo mkfs -t ext4 $disk 

sudo mkdir $diskDir 
sudo mkdir $flashDir 

sudo mount $flash $flashDir 
sudo mount $disk $diskDir 