#!/bin/sh
#url = gjfstmp2/M00/00/00/wKgCzFKMKrWIcKyXAALU2e46dY0AAAAPgOqpiEAAtTx579_0-0_9-version.jpg,    version -> [0, 1000]

#COUNT=10000
##clear
#ROOTDIR=/home/sunfei/image-cache/
#cd $ROOTDIR
#pid=`ps -ef|grep pic_server_fcgi|awk '{print $2}'|head -n 1`
#kill -9 $pid
#sh clear.sh
#echo $pid
#nohup /usr/local/webserver/spawn-fcgi/bin/spawn-fcgi -a 127.0.0.1 -p 15434 -u sunfei -g sunfei -F 1 -n -f /home/sunfei/image-cache/pic_server_fcgi &
header_file=header.$$
body_file=body.$$

function get_loop() {
    start=$1
    end=$2
    for((i=$start;i<$end;++i)) {
        wget --save-header http://127.0.0.1/gjfstmp2/M00/00/00/wKgCzFKMKrWIcKyXAALU2e46dY0AAAAPgOqpiEAAtTx579_10-10_9-$i.jpg -o $header_file -O $body_file
        if [ "`fgrep '200 OK' $header_file`" == "" ]; then
            echo "download $i failed"
        fi
    }
}

#for((i=0;i<10;++i)){
#    wget --save-header http://127.0.0.1/gjfstmp2/M00/00/00/wKgCzFKMKrWIcKyXAALU2e46dY0AAAAPgOqpiEAAtTx579_10-10_9-$i.jpg -o $header_file -O $body_file
#    if [ "`fgrep '200 OK' $header_file`" == "" ]; then
#        echo "download $i failed"
#    fi
#}
#for((i=0;i<10;++i)){
#    wget --save-header http://127.0.0.1/gjfstmp2/M00/00/00/wKgCzFKMKrWIcKyXAALU2e46dY0AAAAPgOqpiEAAtTx579_10-10_9-$i.jpg -o $header_file -O $body_file
#    if [ "`fgrep Hit $body_file`" == "" ]; then
#        echo failed
#    fi
#}
get_loop 0 100
sudo date -s "`date -d "1 days" +"%Y-%m-%d %H:%M:%S"`" 
get_loop 100 200
sudo date -s "`date -d "1 days" +"%Y-%m-%d %H:%M:%S"`" 
get_loop 200 300
sudo date -s "`date -d "1 days" +"%Y-%m-%d %H:%M:%S"`" 
get_loop 300 400
sudo date -s "`date -d "1 days" +"%Y-%m-%d %H:%M:%S"`" 
get_loop 400 500
sudo date -s "`date -d "1 days" +"%Y-%m-%d %H:%M:%S"`" 
get_loop 500 600
sudo date -s "`date -d "1 days" +"%Y-%m-%d %H:%M:%S"`" 
get_loop 600 700
sudo date -s "`date -d "1 days" +"%Y-%m-%d %H:%M:%S"`" 
get_loop 700 800
sudo date -s "`date -d "1 days" +"%Y-%m-%d %H:%M:%S"`" 
get_loop 800 900
sudo date -s "`date -d "1 days" +"%Y-%m-%d %H:%M:%S"`" 
get_loop 900 1000
sudo date -s "`date -d "1 days" +"%Y-%m-%d %H:%M:%S"`" 
get_loop 1000 1100
sudo date -s "`date -d "1 days" +"%Y-%m-%d %H:%M:%S"`" 
get_loop 1100 1200
rm -f $header_file
rm -f $body_file
