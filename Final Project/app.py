from MovieReccomendationSystemEngine import MovieReccomendationSystemEngine
from flask import Blueprint
main = Blueprint('main', __name__)
 
import json

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import numpy as np
from flask import Flask, request
import threading
import atexit

@main.route("/reccommendations/user/<string:userId>/top/<int:count>", methods=["GET"])
def topRatings(userId, count):
    logger.debug("User %s TOP %s ratings requested", userId, count)
    reccommendations = recommendation_engine.getTopRecommendations(userId,count)
    return json.dumps(reccommendations)

@main.route("/predict/user/<string:userId>/movie/<int:movieId>", methods=["GET"])
def predictRatings(userId, movieId):
    logger.debug("User %s rating requested for movie %s", userId, movieId)
    ratings = recommendation_engine.predictRating(userId, movieId)
    return json.dumps(ratings)

@main.route("/search/movie/<string:movie>", methods=["GET"])
def searchTMDBMovie(movie):
    logger.debug("User requested search for string  %s", movie)
    movies = recommendation_engine.searchTMDBMovie(movie)
    return json.dumps(movies)

@main.route("/add/user/<string:userId>/movie/<int:movieId>/rating/<float:rating>", methods = ["POST"])
def addRating(userId, movieId,rating):
    logger.debug("User %s posted %s rating for movie Id %s", userId, rating, movieId)
    result = recommendation_engine.addRating(userId,movieId, rating)
    return result

@main.route("/add/user/<string:userId>", methods = ["POST"])
def addUser(userId):
    logger.debug("Adding new User %s", userId)
    result = recommendation_engine.addUser(userId)
    return result
@main.route('/', methods=['GET'])
def ip():
    return request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

@main.route("/ratings/user/<string:userId>", methods = ["GET"])
def getUserRatings(userId):
    logger.debug("Getting ratings for User %s", userId)
    result = recommendation_engine.getUserRatings(userId)
    return json.dumps(result)

def create_app(spark_context):
    global recommendation_engine
    rank_list = range(1,31)
    iterations_list = range(10,31)
    reg_param_list = np.arange(0.01, 0.3, 0.01)
    ReTrain_Time = 600 #Seconds
    
    #Parameters To change:
    hdfsDir = "hdfs://dumbo/user/ra2630/"
    
    yourThread = threading.Thread()
    dataLock = threading.Lock()
    
    
    def interrupt():
        global yourThread
        yourThread.cancel()
    
    def doStuff():
        new_recommendation_engine = MovieReccomendationSystemEngine(spark_context, \
                hdfsDir, \
                findBestParameters = False, \
                forceTrainModel = True, \
                rank_list = rank_list, \
                iterations_list = iterations_list, \
                reg_param_list = reg_param_list)
        global recommendation_engine 
        recommendation_engine = new_recommendation_engine
        
        yourThread = threading.Timer(ReTrain_Time, doStuff, ())
        yourThread.start()
    
    
    def doStuffStart():
        # Do initialisation stuff here
        global yourThread
        # Create your thread
        yourThread = threading.Timer(ReTrain_Time, doStuff, ())
        yourThread.start()
 
    recommendation_engine = MovieReccomendationSystemEngine(spark_context, \
        "hdfs://dumbo/user/ra2630/", \
        findBestParameters = False, \
        forceTrainModel = False, \
        rank_list = rank_list, \
        iterations_list = iterations_list, \
        reg_param_list = reg_param_list)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    doStuffStart()
    # When you kill Flask (SIGTERM), clear the trigger for the next thread
    atexit.register(interrupt)
    return app

