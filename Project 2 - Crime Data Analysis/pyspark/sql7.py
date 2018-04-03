from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext('local','example')  # if using locally
sqlContext = HiveContext(sc)

ret = sqlContext.sql("select count(*) from pdi p1, pdi p2 where p1.incidntnum = p2.incidntnum").collect()
print(ret)