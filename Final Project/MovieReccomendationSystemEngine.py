#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 25 13:23:17 2017

@author: Rajatagarwal

PART - 2

"""


from pyspark.mllib.recommendation import ALS
from pyspark.sql import SQLContext
import os
import math
import numpy as np
from time import time
from pyspark.mllib.recommendation import MatrixFactorizationModel
from pyspark.sql import Row
import subprocess
import requests
import json
import urllib
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




def run_cmd(args_list):
    """
    run linux commands
    """
    logger.info('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err


class MovieReccomendationSystemEngine:
    def getSelf(self):
        return self
    
    def findBestTrainedModel(self, trainingData_RDD,validationData_RDD,validationSetKey):
        seed = 5L
        min_error = float('inf')
        best_rank = -1
        best_iteration = -1
        best_reg_param = -1    
        for rank in self.rank_list:
            for iteration in self.iterations_list:
                for reg_param in self.reg_param_list:
                    model = ALS.train(trainingData_RDD, rank, seed = seed, iterations = iteration, lambda_ = reg_param)
                    predictions = model.predictAll(validationSetKey).map(lambda r: ((r[0], r[1]), r[2]))
                    actualRatingsAndPredictions = validationData_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
                    error = math.sqrt(actualRatingsAndPredictions.map(lambda r: (r[1][0] - r[1][1])**2).mean())
                    """
                    logger.info("For Model trained with Rank = %s Iteration = %s and Regularization Parameter = %s,  Error = %s" % (rank, iteration,reg_param,error))
                    """
                    LOGGER.info("For Model trained with Rank = %s Iteration = %s and Regularization Parameter = %s,  Error = %s" % (rank, iteration,reg_param,error))
                    if error < min_error:
                        best_rank = rank
                        best_reg_param = reg_param
                        best_iteration = iteration
                        min_error = error
        return (best_rank, best_reg_param, best_iteration, min_error, predictions)
    
    def findBestParams(self):
        (ratingsDataRDD,moviesDataRDD) = self.getData(self.smallDatasetPath)
        
        trainingData_RDD, validationData_RDD = ratingsDataRDD.randomSplit([7, 3], seed=0L)
        validationSetKey = validationData_RDD.map(lambda x: (x[0], x[1]))    
        (best_rank, best_reg_param, best_iteration, min_error, predictions) = self.findBestTrainedModel(trainingData_RDD,validationData_RDD,validationSetKey)
        logger.info("Best Model is trained with Rank %s Iteration %s and Regularization Parameter %s" % (best_rank, best_iteration,best_reg_param))
        
        return (best_rank, best_reg_param, best_iteration)
    
    def searchTMDBMovie(self, movie):
        url = 'https://api.themoviedb.org/3/search/movie?include_adult=true&page=1&query=' + urllib.quote(movie) + '&language=en-US&api_key=5f5fac54ffeb39ac2e2437bc1ee71a50'
        r = requests.get(url)
        data = r.json()
        results = []
        for i in range(len(data['results'])):
            results.append(("Title - " + data['results'][i]["original_title"].encode('utf-8') , "ID - " + str(data['results'][i]["id"]), "Release Date - " + data['results'][i]["release_date"].encode('utf-8')))
        return results
        
    
    def trainModel(self):
        logger.info("Inititaing Model Train definition")
        logger.info("There are %s movies in the complete dataset" % (self.moviesDataRDD.count()))
        start = time()
        ALS.checkpointInterval = 2
        trainedRecommendationModel = ALS.train(self.ratingsDataRDD, self.best_rank, seed=self.seed, iterations = self.best_iteration, lambda_ = self.best_reg_param) 
        duration = time() - start
        logger.info("Model trained in %s seconds" % round(duration,3))
        self.trainedRecommendationModel = trainedRecommendationModel
        
    def saveModel(self):
        logger.info("Saving trained Model to HDFS directory %s" % (self.savedModelPath))
        (s_return, s_output, s_err) = run_cmd(['hdfs', 'dfs', '-ls', self.savedModelPath])
        modelExists = "No such file or directory" not in s_err
        if modelExists:
            logger.info("Deleting previously Saved model")
            run_cmd(['hdfs', 'dfs', '-rm','-r', self.savedModelPath])
            logger.info("Previously Saved model deleted successfully")
        
        self.trainedRecommendationModel.save(self.sc, self.savedModelPath)
        logger.info("Model Saved Successfully")
    
    def getData(self, dataPath):
        ratingsFile = os.path.join(dataPath,"ratings.csv")
        moviesFile = os.path.join(dataPath,"movies.csv")
        logger.info("Loading %s File" %(ratingsFile))
        ratingsRawData = self.sc.textFile(ratingsFile)
        ratingsHeaderLine = ratingsRawData.take(1)[0]
        ratingsDataRDD = ratingsRawData.filter(lambda line: line!= ratingsHeaderLine).map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1],tokens[2])).cache()
        logger.info("File %s loaded successfully" %(ratingsFile))
        logger.info("Loading %s File" %(moviesFile))
        moviesRawData = self.sc.textFile(moviesFile)
        moviesHeaderLine = moviesRawData.take(1)[0]
        moviesDataRDD = moviesRawData.filter(lambda line: line!= moviesHeaderLine).map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1],tokens[2])).cache()
        logger.info("File %s loaded successfully" %(moviesFile))
        return(ratingsDataRDD,moviesDataRDD)
        
    def getNewData(self,dataPath):
        logger.info("Loading %s File" %(dataPath))
        ratingsRawData = self.sc.textFile(dataPath).cache()
        ratingsHeaderLine = ratingsRawData.take(1)[0]
        ratingsDataRDD = ratingsRawData.filter(lambda line: line!= ratingsHeaderLine).map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1],tokens[2])).cache()        
        logger.info("File %s loaded successfully" %(dataPath))
        return ratingsDataRDD
        
    def __init__(self, \
                 sc, \
                 hdfsHomeDirectoryPath, \
                 forceTrainModel = True, \
                 findBestParameters = True, \
                 rank_list = [4,8,12], \
                 iterations_list = range(10,30), \
                 reg_param_list = np.arange(0.01, 0.1, 0.01)):
        
        logger.info("Initializing Reccomendation System Engine")
        self.sc = sc
        self.sqlContext = SQLContext(self.sc)
        self.best_rank = 8
        self.best_iteration = 30
        self.best_reg_param = 0.1
        self.seed = 5L
        
        self.rank_list = rank_list
        self.iterations_list = iterations_list
        self.reg_param_list = reg_param_list
        
        self.fullDatasetPath = os.path.join(hdfsHomeDirectoryPath,"project/MovieLensData/ml-latest")
        self.smallDatasetPath = os.path.join(hdfsHomeDirectoryPath,"project/MovieLensData/ml-latest-small")
        self.savedModelPath = os.path.join(hdfsHomeDirectoryPath,"project/savedRecommendationModel/")
        self.newDataSetPath = os.path.join(hdfsHomeDirectoryPath,"project/newData")
        self.setCheckpointDir = os.path.join(hdfsHomeDirectoryPath,"project/checkpoint/")
        self.sc.setCheckpointDir(self.setCheckpointDir)
        
        (s_return, s_output, s_err) = run_cmd(['hdfs', 'dfs', '-ls', self.savedModelPath])
        modelExists = "No such file or directory" not in s_err
        
        
        (self.ratingsData,self.moviesDataRDD) = self.getData(self.fullDatasetPath)
        self.newRatingsDataRDD = self.getNewData(os.path.join(self.newDataSetPath,'newRatings'))
        self.ratingsDataRDD = self.ratingsData.union(self.newRatingsDataRDD)
        self.movieTitles = self.moviesDataRDD.map(lambda x: (int(x[0]),x[1]))
        
        (s_return, s_output, s_err) = run_cmd(['hdfs', 'dfs', '-ls', self.savedModelPath])
        modelExists = "No such file or directory" not in s_err
        if forceTrainModel or (not modelExists):
            if forceTrainModel:
                logger.info("Forced Train Model is true. Initiating model training")
            else:
                if not modelExists:
                    logger.info("Saved Model doesn't exists in the given path %s. Starting training model." %(self.savedModelPath))
            
            logger.info("Find Best Training parameters is set to : %s" % (findBestParameters))
            
            if findBestParameters:
                logger.info("Finding Best Parameters for the model")
                logger.info("Test List for rank = %s, \
                            \nTest Range for iterations = %s, \
                            \nTest Range for Regularization Parameters = %s" \
                            %(self.rank_list, self.iterations_list, self.reg_param_list))
                
                (self.best_rank,self.best_reg_param,self.best_iteration) = self.findBestParams()
            else:
                logger.info("Using pre-defined values for best_iteration, best_rank and best reg_param")
                logger.info("Best rank = %s, \
                            \nBest reg_param = %s, \
                            \nBest iteration = %s" \
                            %(self.best_rank, self.best_reg_param, self.best_iteration))
            self.trainModel()
            self.saveModel()
            
        else:
            self.trainedRecommendationModel = MatrixFactorizationModel.load(self.sc, self.savedModelPath)
            self.trainedRecommendationModel.productFeatures().cache() 
        
    def printRecommendations(self,rec_movies):
        print ('TOP recommended movies:\n%s' % '\n'.join(map(str, rec_movies)))
    
    def invalidUser(self,userId):
        userId = int(userId)
        return userId not in self.ratingsDataRDD.map(lambda r : (int)(r[0])).distinct().collect()
    
    def invalidMovie(self,movieId):
        return movieId not in self.moviesDataRDD.map(lambda r : (int)(r[0])).collect()
    
    def invalidRating(self,rating):
        rating = (float)(rating)
        if rating < 0 or rating > 5:
            return True
        return False
        
    def ratingExists(self,userId,movieId,newRatingsRaw):
        header = newRatingsRaw.take(1)
            
        
        
    def addUser(self, username):
        file = os.path.join(self.newDataSetPath,'newUsers')
        rawData = self.sc.textFile(file).cache()
        headerLine = rawData.take(1)[0]
        dataRDDWithoutHeader = rawData.filter(lambda line: line!= headerLine).map(lambda line: line.split(","))
        dataRDDWithHeader = rawData.map(lambda line: line.split(","))
        if username in dataRDDWithHeader.map(lambda r : r[0]).collect():
            return "UserName already exists. Please select a new userName"
        if(dataRDDWithoutHeader.count() == 0):
            maxId = 0
        else:
            maxId = (int)(dataRDDWithoutHeader.max()[1])
        newData  = [[username,maxId+1]]
        df1 = self.sqlContext.createDataFrame(dataRDDWithHeader.union(self.sc.parallelize(newData)))
        df1.coalesce(1).write.format('com.databricks.spark.csv').save(path=file, mode="overwrite", sep=',')
        return "User Added Successfully"
    
    def getIdFromUser(self,username):
        file = os.path.join(self.newDataSetPath,'newUsers')
        rawData = self.sc.textFile(file).cache()
        headerLine = rawData.take(1)[0]
        dataRDDWithoutHeader = rawData.filter(lambda line: line!= headerLine).map(lambda line: line.split(","))
        row = dataRDDWithoutHeader.filter(lambda line : line[0] == username)
        if(row.count() == 0):
            logger.info("Id for user %s not found" %(username))
            return -1
        else:
            id = row.collect()[0][1]
            logger.info("Id for user %s found - %s" %(username,id))
            return int(id)
    
    def getMovieIdFromTMDBId(self, id):
        file1 = os.path.join(self.fullDatasetPath,"links.csv")
        file2 = os.path.join(self.newDataSetPath,"newLinks/")
        
        rawData = self.sc.textFile(file1)
        headerLine = rawData.take(1)[0];
        
        oldLinksRDD = rawData.filter(lambda line : line != headerLine).map(lambda line : line.split(',')).map(lambda tokens : (tokens[0], tokens[1], tokens[2]))
        newLinksRDD = self.sc.textFile(file2).filter(lambda line : line != headerLine).map(lambda line : line.split(',')).map(lambda tokens : (tokens[0], tokens[1], tokens[2]))
        
        combinedLinksRDD = oldLinksRDD.union(newLinksRDD)
        filteredRDD = combinedLinksRDD.filter(lambda line : line[2] != '' and int(line[2]) == id)
        if(filteredRDD.count() > 0):
            return int(filteredRDD.collect()[0][0])
        else:
            return -1
    
        
        
        
    def getTopRecommendations(self,username,numOfRecommendations):
        id = self.getIdFromUser(username)
        if id == -1:
            return "User doesn't exists"
        logger.info("Id = %s" %(id))
        return self.getTopRecommendations2(id,numOfRecommendations)
        
        
    def getTopRecommendations2(self,userId,numOfRecommendations):
        if(self.invalidUser(userId)):
            return "User has 0 ratings so far !!!"
        targetUserRDD = self.ratingsDataRDD.filter(lambda r : (int)(r[0]) == userId)
        targetUserRatedMoviesId = targetUserRDD.map(lambda x: (int)(x[1])).collect()
        targetUserUnratedMoviesRDD = (self.moviesDataRDD.filter(lambda x: (int)(x[0]) not in targetUserRatedMoviesId).map(lambda x: (userId, x[0])))
        targetUserRecommendationsRDD = self.trainedRecommendationModel.predictAll(targetUserUnratedMoviesRDD)
        targetUserRecommendationsRatingsRDD = targetUserRecommendationsRDD.map(lambda x: (x.product, x.rating))
        targetUserRecommendationsRatingsAndTitleRDD = targetUserRecommendationsRatingsRDD.join(self.movieTitles).map(lambda r: (r[0],r[1]))
        return targetUserRecommendationsRatingsAndTitleRDD.takeOrdered(numOfRecommendations, key=lambda x: -x[1][0])
    
    def predictRating(self,username,movieId):
        id = self.getIdFromUser(username)
        if id == -1:
            return "User doesn't exists"
        logger.info("User Id = %s" %(id))
        movieId = self.getMovieIdFromTMDBId(movieId)
        if movieId == -1:
            return "Specified Movie ID doesn't exists"
        logger.info("Movie Id = %s" %(movieId))
        return self.predictRating2(id,movieId)
    
    
    def predictRating2(self,userId,movieId):
        if(self.invalidUser(userId)):
            return "User has 0 ratings so far !!!"
        if(self.invalidMovie(movieId)):
            return "Movie Id provided does not exists"
        targetUserRDD = self.ratingsDataRDD.filter(lambda r : (int)(r[0]) == userId)
        targetUserRatedMoviesId = targetUserRDD.map(lambda x: (int)(x[1])).collect()
        if movieId in targetUserRatedMoviesId:
            return self.ratingsDataRDD.filter(lambda x: (int)(x[0]) == userId and (int)(x[1]) == movieId).map(lambda r : ((int)(r[1]),(float)(r[2]))).join(self.movieTitles).collect()
        targetUserUnratedMoviesRDD = self.moviesDataRDD.filter(lambda x: (int)(x[0]) == movieId).map(lambda x: (userId, x[0]))
        targetUserRecommendationsRDD = self.trainedRecommendationModel.predictAll(targetUserUnratedMoviesRDD)
        targetUserRecommendationsRatingsRDD = targetUserRecommendationsRDD.map(lambda x: (x.product, x.rating))
        targetUserRecommendationsRatingsAndTitleRDD = targetUserRecommendationsRatingsRDD.join(self.movieTitles).map(lambda r: (r[0],r[1]))
        return targetUserRecommendationsRatingsAndTitleRDD.takeOrdered(1, key=lambda x: -x[0])
    
    
    
    
    def addRating(self, username, movieId, rating):
        if(self.invalidRating(rating)):
            return "Rating must be a number between between 0 and 5."
        userId = self.getIdFromUser(username)
        if userId == -1:
            return "User doesn't exists"
        movieId = self.getMovieIdFromTMDBId(movieId)
        if movieId == -1:
            return "Specified Movie ID doesn't exists"
        
        
        file = os.path.join(self.newDataSetPath,"newRatings/")
        logger.info("here")
        ratingsData = self.sc.textFile(file).map(lambda l: l.split(",")).cache()
        row_data = ratingsData.map(lambda p: Row(userId=p[0], movieId=p[1], rating=p[2], timestamp=p[3]))
        ratings_df = self.sqlContext.createDataFrame(row_data)
        ratings_df.registerTempTable("ratings")
        
        
        alreadyRated = self.sqlContext.sql('SELECT * from ratings where userId = "{}" and movieId = "{}"'.format(userId,movieId))
        
        if alreadyRated.count() > 0:
            logger.info("Rating for given movie and user already exists. Updating the rating !!!")
            newRatingsRDD = self.sqlContext.sql('SELECT userId, movieId, case when userId = "{}" and movieId = "{}" then "{}" else rating end as rating, case when userId = "{}" and movieId = "{}" then "{}" else timestamp end as timestamp from ratings'.format(userId,movieId,rating,userId,movieId,time())).rdd.cache() 
        else:
            logger.info("Adding new Rating !!!")
            newData  = [(userId,movieId,rating,time())]
            newDataRDD = self.sc.parallelize(newData)
            newRatingsRDD = ratingsData.union(newDataRDD).cache()
        
        df1 = self.sqlContext.createDataFrame(newRatingsRDD)
        df1.coalesce(1).write.format('com.databricks.spark.csv').save(path=file, mode="overwrite", sep=',')
        
        movie = self.movieTitles.filter(lambda line : int(line[0]) == movieId).collect()[0][1].encode('utf-8')
        if alreadyRated.count() > 0:
            return "Rating for movie " + movie +" updated successfully !!"
        else:
            return "Rating for movie " + movie + " added successfully !!"
        
    
    def getUserRatings(self, username):
        userId = self.getIdFromUser(username)
        if userId == -1:
            return "User doesn't exists"
        file = os.path.join(self.newDataSetPath,"newRatings/")
        rawData = self.sc.textFile(file)
        headerLine = rawData.take(1)[0]
        ratingsData = rawData.filter(lambda line: line!= headerLine).map(lambda line: line.split(",")).filter(lambda line : int(line[0]) == userId).map(lambda tokens : (int(tokens[1]),tokens[2]))
        if(ratingsData.count() > 0):
            return self.movieTitles.join(ratingsData).map(lambda line : (line[1][0].encode('utf-8') , float(line[1][1]))).collect()
        else:
            return "No ratings found"
        

    
        



