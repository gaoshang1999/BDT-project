from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext('local','example')  # if using locally
sqlContext = HiveContext(sc)

ret = sqlContext.sql("select  split(time_, ':')[0]  , count(*)  from pdi  group by split(time_, ':')[0] order by 1").collect()
print(ret)