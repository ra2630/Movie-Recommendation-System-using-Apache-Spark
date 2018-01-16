import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkConf, SparkContext
def init_sc():
    conf = SparkConf().setAppName("Movie Recommendation System")
    sc = SparkContext(conf=conf, pyFiles=['MovieReccomendationSystemEngine.py','app.py'])
    return sc

def run_server(app):
 
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)
 
    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')
 
    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })
 
    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()

if __name__ == "__main__":
    sc = init_sc()
    app = create_app(sc)
    app.run(host='0.0.0.0', debug = False)
    
