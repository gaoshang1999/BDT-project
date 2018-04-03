from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext('local','example')  # if using locally
sqlContext = HiveContext(sc)

ret = sqlContext.sql("select substr(time_, 0, 4)||'0' as ten_minutes, count(*) from pdi group by substr(time_, 0, 4)||'0' order by 1").collect()
print(ret)