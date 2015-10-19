#!/bin/sh
ulimit -c unlimited
#exec valgrind --tool=helgrind  --trace-children=yes --leak-check=full -v --log-file=memcheck /home/mawenlong/work/cc_dev/src/ganji/pic_server_fcgi/pic_server_fcgi
exec valgrind --tool=memcheck -q --time-stamp=yes --trace-children=yes --leak-check=full -v --log-file=memcheck /home/mawenlong/work/cc_dev/src/ganji/pic_server_fcgi/pic_server_fcgi
#exec valgrind --tool=memcheck  --trace-children=yes --leak-check=full -v --log-file=memcheck /home/mawenlong/work/cc_dev/src/ganji/pic_server_fcgi/pic_server_fcgi
#exec valgrind --tool=memcheck  --show-reachable=yes --trace-children=yes --num-callers=20 --leak-resolution=high --leak-check=full -v --log-file=memcheck /home/mawenlong/work/cc_dev/src/ganji/pic_server_fcgi/bin/pic_server_fcgi

valgrind --tool=memcheck -q --time-stamp=yes --trace-children=yes --leak-check=full -v --log-file=memcheck /usr/local/webserver/spawn-fcgi/bin/spawn-fcgi -a 127.0.0.1 -p 15434 -u sunfei -g sunfei -F 1 -n -f /data/log-replay/cc_dev/trunk/src/ganji/pic_server_fcgi/pic_server_fcgi
