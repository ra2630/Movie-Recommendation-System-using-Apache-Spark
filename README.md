# Movie-Recommendation-System-using-Apache-Spark---on-NYU-Dumbo-Cluster-Only

1.	Create a new directory (Any Name)
2.	Copy contents of ProjectFile to the directory
a.	app.py – Contains the Web App interface of the application. It contains the definition of the GET and POST calls, and multithreaded Timer tasks for the recommender systems.
To execute application, make change here, set hdfsDir= "hdfs://dumbo/user/ra2630" - Edit the HDFS home directory path (change username from ra2630 to your username if using Dumbo, else give an absolute path to the HDFS directory as shown above)

b.	exportAdditionalData.sh – Script to export additional data to HDFS dir if it doesn’t exists
c.	exportMovieLensDataToHDFS.sh– Script to export MovieLensData to HDFS
d.	importMovieLensData.py – Script to download MovieLensData from internet and unzip it in current local directory
e.	MovieReccomendationSystemEngine.py – The main system engine. Contains methods of the recommendation system and training models.
f.	newRatings.csv – Additional File
g.	newUsers.csv – Additional File
h.	server.py – Python code to setup the server.
i.	API Calls.docx – Document containing API Calls with example.
j.	Runner.sh – Executable code to start the server
3.	Execute the following commands to give executable permission to shell script files :
chmod +x exportMovieLensDataToHDFS.sh
chmod +x exportditionalDataToHDFS.sh
chmod +x Runner.sh
*** Don’t forget to change the hdfs directory in app.py…

4.	Execute Runner.sh file from the directory where all files are stored to start the server
