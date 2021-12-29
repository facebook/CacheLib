disk=$1
flash=$2

diskDir=$3
flashDir=$4

sudo mkfs -t ext4 $flash 
sudo mkfs -t ext4 $disk 

sudo mkdir $diskDir 
sudo mkdir $flashDir 

sudo mount $flash $flashDir 
sudo mount $disk $diskDir 