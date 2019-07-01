
"""
recipe_etl_job.py
~~~~~~~~~~
This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'

Approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment

"""

import os

import sys
sys.path.append(os.getcwd())
import config.arg as arg
from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.column import _to_java_column
from pyspark.sql.column import _to_seq
import pyspark.sql.functions as F
from pyspark.sql.functions import col,udf,lit,lower,unix_timestamp
from dependancies.loggingSession import *
from dependancies.sparkSession import *
import config.recipes_etl_column_mapping as recipes_etl_column_mapping
from config.cfg import config

import time
import datetime
timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

#start Spark application and get Spark session, logger
logger=getloggingSession()
spark=getSparkSession()

sc=spark.sparkContext

def string_conversion(col):
	_string_lenghth=sc._jvm.com.spark.etl.driver.configurator.getJoda()
	return Column(_string_lenghth.apply(_to_seq(sc,[col],_to_java_column)))
	
class recipe_etl:
	def extract_input_file(self,input_path):
		"""Load data from json file format.
		:param spark: input_path.
		:return: Spark DataFrame.
		"""
		try:
			logger.info("Reading Input Json File ...!!!")
			input_file=spark.read.json(input_path)
			logger.info("Input File Read Successfully")
			return input_file
		except Exception as e:
			logger.info("Failed to Read Input File!!!")
			logger.exception("Error in extract_input_file function " + str(e))
			sys.exit(400)
	
	def transform_input_file(self,input_file):
		"""Transform original dataset.
		:param df: Input DataFrame.
		:return: Transformed DataFrame.
		"""
		try:
			logger.info("Performing the Transformation on the input file ...")
			input_file1=input_file\
				.select(recipes_etl_column_mapping.recipes_etl_ingredients,recipes_etl_column_mapping.recipes_etl_prepTime,recipes_etl_column_mapping.recipes_etl_cookTime)\
				.filter(lower(col(recipes_etl_column_mapping.recipes_etl_ingredients)).contains("beef"))\
				.withColumn("new_prep_time",string_conversion(col(recipes_etl_column_mapping.recipes_etl_prepTime)).cast("Int"))\
				.withColumn("new_cook_time",string_conversion(col(recipes_etl_column_mapping.recipes_etl_cookTime)).cast("Int"))\
				.withColumn(recipes_etl_column_mapping.recipes_etl_difficulty,F.when((col("new_prep_time") + col("new_cook_time") >= 3600),F.lit("HARD"
				))\
					.otherwise(\
						F.when((col("new_prep_time") + col("new_cook_time") >= 1800),F.lit("MEDIUM"))\
							.otherwise(\
								F.when((col("new_prep_time") + col("new_cook_time") <=1800),F.lit("EASY"))\
									.otherwise(F.lit("UNKNOWN")))))\
				.withColumn('current_timestamp',unix_timestamp(F.lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))\
				.drop(col("new_cook_time"))\
				.drop(col("new_prep_time"))
			input_file1.printSchema()
			input_file1.show(100)
			logger.info("Successfully Transfomed the dataframe")
			return input_file1
		except Exception as e:
			logger.info("Failed to transform Input File!!!")
			logger.exception("Error in transform_input_file function " + str(e))
			sys.exit(400)
			
	def replace_or_create_table(self,output_path,drop=True):
		"""Create the Input Table..
		:param df: output_path,default: drop=True.
		:return: None.
		"""
		try:
			spark.sql('use ' + config.get('hive','schema'))
			if ( drop is False) & (config.get('hive','recipes_etl_data') in spark.sql('show tables').toPandas()['tableName'].values):
				print '{} Exists. Pass True as final flag or drop it manually'.format(config.get('hive','recipes_etl_data'))
			else:
				spark.sql('DROP TABLE IF EXISTS {}'.format(config.get('hive','schema') + "." + config.get('hive','recipes_etl_data')))
				schema=recipes_etl_column_mapping.recipes_etl_ingredients + " string, " + recipes_etl_column_mapping.recipes_etl_prepTime + " string," + recipes_etl_column_mapping.recipes_etl_cookTime + " string," + recipes_etl_column_mapping.recipes_etl_difficulty + " string," + "execution_time" + " timestamp"
				print schema
				spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS  "+  config.get('hive','schema') + "." + config.get('hive','recipes_etl_data') + "(" + schema + ")" + " STORED AS PARQUET LOCATION " + "'" + output_path  + "'")
			
		except Exception as e:
			logger.info("Failed to transform Input File!!!")
			logger.exception("Error in transform_input_file function " + str(e))
			sys.exit(400)
			
	def load_input_file(self,input_file,output_path):
		"""Collect data locally and write to CSV.
		:param df: input DataFrame,Output file path.
		:return: None
		"""
		try:
			logger.info(" Loading the transform_input_file to impala extrnal location ...")
			output_path_difficulty_hard=output_path + recipes_etl_column_mapping.recipes_etl_difficulty + "=" + "HARD"
			output_path_difficulty_madium=output_path + recipes_etl_column_mapping.recipes_etl_difficulty + "=" + "MEDIUM"
			output_path_difficulty_easy=output_path + recipes_etl_column_mapping.recipes_etl_difficulty + "=" + "EASY"
			output_path_difficulty_unknown=output_path + recipes_etl_column_mapping.recipes_etl_difficulty + "=" + "UNKNOWN"
			
			
			output_path_difficulty_hard_df=input_file.filter(col(recipes_etl_column_mapping.recipes_etl_difficulty) =="HARD")
			output_path_difficulty_madium_df=input_file.filter(col(recipes_etl_column_mapping.recipes_etl_difficulty) =="MEDIUM")
			output_path_difficulty_easy_df=input_file.filter(col(recipes_etl_column_mapping.recipes_etl_difficulty) =="EASY")
			output_path_difficulty_unknown_df=input_file.filter(col(recipes_etl_column_mapping.recipes_etl_difficulty) =="UNKNOWN")
			
			output_path_difficulty_hard_df.write.mode("overwrite").parquet(output_path_difficulty_hard)
			output_path_difficulty_madium_df.write.mode("overwrite").parquet(output_path_difficulty_madium)
			output_path_difficulty_easy_df.write.mode("overwrite").parquet(output_path_difficulty_easy)
			output_path_difficulty_unknown_df.write.mode("overwrite").parquet(output_path_difficulty_unknown)
			
		except Exception as e:
			logger.info("Failed to load Input File!!!")
			logger.exception("Error in load_input_file function " + str(e))
			sys.exit(400)
	
class task_executor(recipe_etl):
	def __init__(self,tasks):
		self.tasks=tasks
	
	def run(self):
		logger.info(" Starting the ETL Process")
		# execute ETL pipeline
		logger.info(self.tasks)
		
		input_file_path="/user/data/recipes.json"
		output_file_path="/user/data/output/"
		
		if self.tasks=="extract":
			input_file=recipe_etl.extract_input_file(self,input_file_path)
			input_file.show(10)
		elif self.tasks=="transform":
			input_file=recipe_etl.extract_input_file(self,input_file_path)
			transformed_file=recipe_etl.transform_input_file(self,input_file)
			transformed_file.show(10)
		elif self.tasks=="create_table":
			recipe_etl.replace_or_create_table(self,output_file_path)
		else:
			input_file=recipe_etl.extract_input_file(self,input_file_path)
			transformed_file=recipe_etl.transform_input_file(self,input_file)
			transformed_file=recipe_etl.load_input_file(self,transformed_file,output_file_path)
			
		#log the success and terminate Spark application
		logger.info('test_etl_job is finished')
		spark.stop()
		
# entry point for PySpark ETL application
if __name__=="__main__":
	"""Main ETL script definition.
	:return: None
	"""
	 # log that main ETL job is starting
	try:
		logger.info("Recipe ETL Process has been Started !!!")
		arg.parser.add_argument('task_name',help="task_name",type=str)
		args, _ = arg.parser.parse_known_args()
		task_name=args.task_name
		etl=task_executor(task_name)
		etl.run()
	except Exception as e:
		logger.info("Failed to load input args!!!")
		logger.exception("Error in main function " + str(e))
		sys.exit(400)
	