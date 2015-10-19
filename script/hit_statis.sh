
awk '{
  if(ARGIND==1) {
    s1[$1]=1
  } else {
    access_avoidsame_count++
    access_avoidsame_size+=$2
    access_count+=$3
    access_size+=$2*$3
    if(($1 in s1)){
      hit_avoidsame_count++;
      hit_avoidsame_size+=$2
      hit_count+=$3
      hit_size+=$2*$3
    }
  }
}
END{
  printf "访问文件数=%d,访问文件大小=%d,去重访问文件数=%d,去重访问文件大小=%d\n", access_count, access_size,access_avoidsame_count,access_avoidsame_size
  printf "命中文件数=%d,命中文件大小=%d,去重命中文件数=%d,去重命中文件大小=%d\n", hit_count, hit_size,hit_avoidsame_count,hit_avoidsame_size
  printf "命中文件数比=%.2f,命中文件大小比=%.2f,去重命中文件数比=%.2f,去重命中文件大小比=%.2f\n", hit_count/access_count, hit_size/access_size,hit_avoidsame_count/access_avoidsame_count,hit_avoidsame_size/access_avoidsame_size
}' $1 $2 
#access_22.log access_10-21.log
#access_22.log access_15-21.log 
#access_img_10-22.log access_10-22.log
