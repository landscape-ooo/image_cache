dayago1=`date +%Y%m%d -d "0 days ago"`
dayago2=`date +%Y%m%d -d "1 days ago"`
dayago3=`date +%Y%m%d -d "2 days ago"`
#dayago1_logfile="/ganji/logs/2011/11/access_$dayago1.log"
dayago1_logfile="/ganji/logs/access.log"
dayago2_logfile="access_$dayago2.log"
dayago3_logfile="access_$dayago3.log"

dayago2_tgzlogfile="/ganji/logs/2011/11/$dayago2_logfile.tgz"
dayago3_tgzlogfile="/ganji/logs/2011/11/$dayago3_logfile.tgz"

dayago1_clusterfile=cluster_$dayago1.log
dayago2_clusterfile=cluster_$dayago2.log
dayago3_clusterfile=cluster_$dayago3.log
if [ ! -f $dayago1_clusterfile ] ; then
	if [ -f $dayago1_logfile ]; then
		awk -F ' ' '{s[$7]++}END{for(k in s) print k;}' $dayago1_logfile |egrep ".(jpg|jpeg|png|bmp|gif)"|sort -k1,1 >$dayago1_clusterfile
	fi
fi

if [ ! -f $dayago2_clusterfile ] ; then
	if [ ! -f $dayago2_logfile ]; then
		tar -xvf $dayago2_tgzlogfile
	fi
	awk -F ' ' '{s[$7]++}END{for(k in s) print k;}' $dayago2_logfile |egrep ".(jpg|jpeg|png|bmp|gif)"|sort -k1,1 >$dayago2_clusterfile
fi

if [ ! -f $dayago3_clusterfile ] ; then
	if [ ! -f $dayago3_logfile ]; then
		tar -xvf $dayago3_tgzlogfile
	fi
	awk -F ' ' '{s[$7]++}END{for(k in s) print k;}' $dayago3_logfile |egrep ".(jpg|jpeg|png|bmp|gif)"|sort -k1,1 >$dayago3_clusterfile
fi

awk '{if(ARGIND<2)s[$1]=1; else { if(!($1 in s)) print $1}}' $dayago1_clusterfile $dayago2_clusterfile  >tmp.$$ && mv tmp.$$ $dayago2_clusterfile
awk '{if(ARGIND<3)s[$1]=1; else { if(!($1 in s)) print $1}}' $dayago1_clusterfile $dayago2_clusterfile $dayago3_clusterfile  >tmp.$$ && mv tmp.$$ $dayago3_clusterfile

cat $dayago1_clusterfile $dayago2_clusterfile $dayago3_clusterfile >file_list.data.all
fgrep gjfs file_list.data.all >file_list.data

