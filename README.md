# HelloFresh Data Engineering Test

Thank you for your interest in joining HelloFresh! As part of our selection process, all of our candidates must take the following test.
The test is designed to assess key competencies required in your role as a data engineer at HelloFresh.

Please submit your answers in a different branch and create a pull request. Please do not merge your own pull request.

_Note: While we love open source here at HelloFresh, please do not create a public repo with your test in! This challenge is only shared with people interviewing, and for obvious reasons we'd like it to remain this way._

# 1. SQL

[Cloudera Impala](http://impala.io/), one of the tools of the Hadoop Ecosystem that allow us to execute SQL queries on Hadoop, provides a range of powerful analytical SQL functions that come very handy when doing analysis on datasets. You can find more information on the analytical SQL functions offered by Impala [here](http://www.cloudera.com/content/www/en-us/documentation/archive/impala/2-x/2-1-x/topics/impala_analytic_functions.html)

Given the following sample table `ingredients` that records how individual records change over time: 
```
------------------------------------------------------------------
| id_ingredient | ingredient_name | price |  timestamp | deleted |
------------------------------------------------------------------
|        1      | potatoes        | 10.00 | 1445899655 |    0    |
|        2      | tomatoes        | 20.00 | 1445836421 |    0    |
|        1      | sweet potatoes  | 10.00 | 1445899132 |    0    |
|        1      | sweet potatoes  | 15.00 | 1445959231 |    0    |
|        2      | tomatoes        | 30.00 | 1445894337 |    1    |
|        3      | chicken         | 50.00 | 1445899655 |    0    |
							....
							....
|       999     | garlic          | 17.00 | 1445897351 |    0    |
------------------------------------------------------------------

```
Write an Impala SQL query that creates a view of that `ingredients` table, that shows only **the most recent state** of each ingredient that **has not been yet deleted**.

Save your answer to a `.sql` file in the root directory of this repository.

# 2. Apache Spark & Python

## Exercise

You need to build a simple application able to run some Tasks in a specific order. To achieve that, you will need an executor taking care of which task to execute, and a base Task class with the common abstraction in **Python**.

As an example:

```python
class Executor(object):
    def __init__(self, tasks=[]):
        self.tasks = tasks
    def run(self):
        # do stuff
```

Your pipeline needs to fulfill these tasks:

- Use the dataset on S3 as the input (https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json)
- Do some transformations.
- Create a table and make the output data ready to be queried after and during each execution using **Impala**, without any manual steps.
- It needs to send an alert if some Task failed, for instance: Send an email, a Slack message or anything else that makes error discovery and error handling easier. You can create just the abstractions for that, or fully implement it, it's up to you.
- The executor should run the whole pipeline or an individual Task if we specify it on the command line as an argument.

> It's up to you how do you organize the tasks, keep in mind that the system might grow in the future so good abstractions and clean code will help you.

### Transformation

You need to extract from the dataset all the recipes that have "*beef*" as one of the ingredients. Then, we need to add a new field (named `difficulty`) calculating the difficulty of the recipe based on:

- **Hard** if the total of `prepTime` and `cookTime` is greater than 1 hour.
- **Medium** if the total is between 30 minutes and 1 hour.
- **Easy** if the total is less than 30 minutes.
- **Unknown** otherwise.

To be able to track changes in the source data over time, add the **date of execution** as a column to the output. Make sure that historical data is not overwritten.

You should store the data in parquet format using the difficulty as partition field.

This task must be done using **Spark** and **Python**.

## Requirements

1. Write well structured (**object oriented**), documented and maintainable code.
2. Write unit tests to test the different components of your solution.
3. Make your solution robust against different kind of failures and **keep in mind** that it should work with bigger data sets.
4. The pipeline should be able to be executed manually from the command line in yarn-client and standalone modes. Add the instructions for the execution to the pipeline's documentation.
5. The system should handle all kinds of errors and react accordingly, for instance, sending an email with the failure.
6. The system should stop if any of the tasks fails.
7. Place your answer in a directory called "recipes-etl" in the root of this repository, with a README.md file that outlines the instructions to run your application.

## Open question

How you would schedule the system in order to run automatically at a certain time?

Open Recipes archive courtesy of [Ed Finkler](https://github.com/fictivekin/openrecipes)

# 3. Data Modelling

ADIS lifts have a system to simulate different lift control mechanisms in their buildings. Let us assume a building has M lifts and N floors.

You are in charge of measuring the performance of different lift control mechanisms and so will need to design the data model to capture observed data and the measurements you would calculate on that data model. Assume a typical simulation run proceeds over a 24 hour period and you are allowed to observe as much as you like (when/where each lift is, how many passengers, where the passengers are, when/where they arrive/depart, etc – if in doubt assume you can observe it). From your detailed data model you will then need (with very simple calculations) to determine various performance measures, for example:

Average waiting time per passenger 

Average journey time per passenger 
… etc 

1. List the different stakeholders who would be interested in lift performance (a “stakeholder” is any person or group who have an interest in or may be affected by the lift performance).**


2. List other performance measures that it would be useful or important to measure – make sure these cover all of the stakeholders. (Hint: there are lots of these).**


3. What would a suitable data representation look like? Please design a series of tables (as would be suitable to put in a database or spreadsheet). Make sure that the data representation (with very simple arithmetic calculations) is adequate to calculate the above measures, and any other measures that you deem important (and that those calculations are fairly easy and unambiguous).**


4. For “Average waiting time per passenger” and at least 2 other performance measures, describe how they can be easily calculated from your data model. Preferably write the SQL code you would use to calculate the waiting and journey times.**


5. Describe a simple but sensible algorithm or set of rules which could run a lift control mechanism. In what ways would this simple lift control mechanism work well, and in what ways might it not work well in the real world? What other complications might be important to turn this into a real-world, operational lift controller?**


Please answer these questions either by editing this file, or by adding an additional document to this repository that contains the answers to these questions.
