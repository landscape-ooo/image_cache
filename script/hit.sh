fgrep NOTICE picserverlog|awk '{print $3,$6,$8}'|awk -F ':' '{
	s1[$1":"$2]++;
	if($5=="1,") {
		s2[$1":"$2]++;
	}
} 
END {
	for(k in s1) {
		printf "%s\t%s\t%s\t%.2f%%\n", k,s1[k], s2[k], s2[k]*1.0/s1[k] * 100;
	}
}' |sort>min.log
