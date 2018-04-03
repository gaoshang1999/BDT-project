from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext('local','example')  # if using locally
sqlContext = HiveContext(sc)

ret = sqlContext.sql("select DayOfWeek, count(*) from pdi  group by DayOfWeek  order by DayOfWeek ='Monday' desc, DayOfWeek ='Tuesday' desc, DayOfWeek ='Wednesday' desc, DayOfWeek ='Thursday' desc, DayOfWeek ='Friday' desc, DayOfWeek ='Saturday' desc, DayOfWeek ='Sunday' desc").collect()
print(ret)