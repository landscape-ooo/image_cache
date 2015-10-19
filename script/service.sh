#!/usr/bin/env bash
Usage="Usage:$0 <start|stop>"
if [ $# -lt 1 ]; then
    echo "$Usage"
    exit
fi
runtype=$1

PWD=`pwd`

## ABSOLUTE path to the spawn-fcgi binary
SPAWNFCGI="/usr/local/webserver/spawn-fcgi/bin/spawn-fcgi"

## ABSOLUTE path to the FastCGI application (php-cgi, dispatch.fcgi, ...)
FCGIPROGRAM="$PWD/bin/pic_server_fcgi"

## if this script is run as root switch to the following user
USERID=nobody
GROUPID=nobody
#FCGI_WEB_SERVER_ADDRS="192.168.64.17"
FCGI_WEB_SERVER_ADDRS="0.0.0.0"
FCGIPORT="15434"
FCGI_CHILDREN=1

exec 1>>service.log 2>&1
start() {
    while true
    do
        HAS=`ps -ef | grep pic_server_fcgi | grep -v grep | awk '{print $2}'`
        if test x$HAS == x; then
            ulimit -c 3000000 
            ulimit -n 300000
            $SPAWNFCGI -a $FCGI_WEB_SERVER_ADDRS -p $FCGIPORT -u $USERID -g $GROUPID -F $FCGI_CHILDREN -f $FCGIPROGRAM 
            echo "`date +"%Y-%m-%d %H:%M:%S"` - process start"
        fi
        sleep 5
    done
}

stop(){
    killall pic_server_fcgi
    echo "`date +"%Y-%m-%d %H:%M:%S"` - process stop"
}


if [ "$runtype" == "start" ] ; then 
    start&
elif [ "$runtype" == "stop" ] ; then
    stop
else 
    echo "$Usage"
fi
