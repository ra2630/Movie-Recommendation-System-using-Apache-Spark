
mkdir MovieLensData
python importMovieLensData.py
./exportMovieLensDataToHDFS.sh
./exportAdditionalDataToHDFS.sh
/usr/bin/spark-submit --total-executor-cores 20 --executor-memory 6g server.py
