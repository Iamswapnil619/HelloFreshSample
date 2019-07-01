"""
This file is exposes a utility function called to get SparkSession to retun a valid spark session

"""

from pyspark.sql import SparkSession
from dependancies.loggingSession import *
import sys

def getSparkSession():
	try:
		logger=getloggingSession()
		spark=SparkSession.builder.appName('recipes-etl').getOrCreate()
		logger.info("sparkSession.py  ->  Completed Successfully")
		return spark
	except Exception as e:
			logger.info(" Unable to Create Spark Session !!!")
			logger.exception("Error in getSparkSession function " + str(e))
			sys.exit(400)