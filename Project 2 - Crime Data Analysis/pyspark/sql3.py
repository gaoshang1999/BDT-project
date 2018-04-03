from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext('local','example')  # if using locally
sqlContext = HiveContext(sc)

ret = sqlContext.sql("select  month(to_date(date_, 'MM/dd/yyyy')) as mon, count(*)  from pdi  group by  month(to_date(date_, 'MM/dd/yyyy')) order by 1").collect()
print(ret)