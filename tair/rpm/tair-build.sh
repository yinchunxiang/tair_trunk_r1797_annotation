#!/bin/bash
#for taobao abs
sudo rpm -ivh http://upload.yum.tbsite.net/alibase/5/x86_64/jsoncpp-0.5-0.x86_64.rpm
temppath=$1
cd $temppath/packages
release=`cat /etc/redhat-release|cut -d " " -f 7|cut -d "." -f 1`
sed -i  "s/^Release:.*$/Release: "$4".el${release}/" $2.spec
sed -i  "s/^Version:.*$/Version: "$3"/" $2.spec
cd $temppath
chmod +x bootstrap.sh
./bootstrap.sh
export TBLIB_ROOT=/opt/csr/common
./configure
make PREFIX=/opt/csr/tair-2.3 TMP_DIR=/home/ads/tmp/tair-tmp.$$ rpms
#make rpms
mv *.rpm rpm/
