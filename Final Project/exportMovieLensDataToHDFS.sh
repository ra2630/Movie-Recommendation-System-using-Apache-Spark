hdfs dfs -test -e project/MovieLensData/ml-latest/ratings.csv
if [ "$?" = 1 ]; then
   echo "File Doesn't Exists, transferring File to HDFS"
   hdfs dfs -rm -r project/MovieLensData/ml-latest
   hdfs dfs -mkdir -p project/MovieLensData/
   hdfs dfs -put -f MovieLensData/ml-latest project/MovieLensData/ml-latest
else
   echo "File Exsists, Not transferring"
fi

hdfs dfs -test -e project/MovieLensData/ml-latest-small/ratings.csv
if [ "$?" = 1 ]; then
   echo "File Doesn't Exists, transferring File to HDFS"
   hdfs dfs -rm -r project/MovieLensData/ml-latest-small
   hdfs dfs -mkdir -p project/MovieLensData/
   hdfs dfs -put -f MovieLensData/ml-latest-small project/MovieLensData/ml-latest-small
else
   echo "File Exsists, Not transferring"
fi

 
