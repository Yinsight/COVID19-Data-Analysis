# COVID19-Data-Analysis

Spark Task1 (Same result as Hadoop Task2):
Example usage:
spark-submit --class SparkCovid19_1 SparkCovid19_1.jar /user/root/covid19_full_data.csv 2020-03-05 2020-04-05 /user/root/sparktask1output
Argument check and error handling are done the same way as in Hadoop task.

Spark Task2 (Same result as Hadoop Task3):
Example usage:
spark-submit --class SparkCovid19_2 SparkCovid19_2.jar /user/root/covid19_full_data.csv /user/root/populations.csv /user/root/sparktask2output
Argument check is done the same way as in Hadoop task.
