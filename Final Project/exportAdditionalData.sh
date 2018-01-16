hdfs dfs -test -e project/newData/newRatings/
if [ "$?" = 1 ]; then
   echo "NewRatings Doesn't Exists, transferring File to HDFS"
   hdfs dfs -rm -r project/newData/newRatings/
   hdfs dfs -mkdir -p project/newData/newRatings/
   hdfs dfs -put -f  newRatings.csv project/newData/newRatings/
else
   echo "File Exsists, Not transferring"
fi

hdfs dfs -test -e project/newData/newUsers/
if [ "$?" = 1 ]; then
   echo "NewUsers Doesn't Exists, transferring File to HDFS"
   hdfs dfs -rm -r project/newData/newUsers/
   hdfs dfs -mkdir -p project/newData/newUsers/
   hdfs dfs -put -f  newUsers.csv project/newData/newUsers/
else
   echo "File Exsists, Not transferring"
fi
 
