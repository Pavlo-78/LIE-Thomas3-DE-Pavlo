rem script for starting
rem the "Dlog4j.level=WARN" options suppress redundant messages
spark-submit ^
  --conf "spark.driver.extraJavaOptions=-Dlog4j.level=WARN" ^
  --conf "spark.executor.extraJavaOptions=-Dlog4j.level=WARN" ^
  diabetes_ML_Spark.py