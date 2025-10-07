#!/bin/bash
#
slice="cpu-only.slice"
if [ "$1" == "0" ]
then
	slice="dsa.slice"
fi

#echo "running $slice"
sudo -E docker run --privileged \
       	-v /sys/bus/dsa:/sys/bus/dsa \
	--device /dev/dsa \
	-v /home/byrnedan/:/workdir \
	-e http_proxy=http://proxy-dmz.intel.com:912 \
	-e https_proxy=http://proxy-dmz.intel.com:912 \
	-e DTO_COLLECT_STATS=1 \
	-e DTO_WAIT_METHOD=busypoll \
	-e DTO_USESTDC_CALLS=$1 \
	--cgroup-parent=$slice \
	-it \
	--entrypoint /bin/bash cachelib:dto
