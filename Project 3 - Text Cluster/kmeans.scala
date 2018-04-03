import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.Normalizer

val sentenceData = spark.sql("select id, text from kmeans_input").toDF("label", "sentence")

val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
val wordsData = tokenizer.transform(sentenceData)
wordsData.show()

val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(10000)
val featurizedData = hashingTF.transform(wordsData)
featurizedData.show()

// alternatively, CountVectorizer can also be used to get term frequency vectors
val idf = new IDF().setInputCol("rawFeatures").setOutputCol("idfFeatures")
val idfModel = idf.fit(featurizedData)
val rescaledData = idfModel.transform(featurizedData)
rescaledData.show()

val normalizer = new Normalizer().setInputCol("idfFeatures") .setOutputCol("features").setP(2.0)
val l1NormData = normalizer.transform(rescaledData)
l1NormData.show()

val dataset =l1NormData.select("features")

val kmeans = new KMeans().setK(10).setSeed(1L)
val model = kmeans.fit(dataset)

model.summary.cluster.withColumn("label",functions.monotonically_increasing_id()+1).write.saveAsTable("kmeans_output")

// Evaluate clustering by computing Within Set Sum of Squared Errors.
val WSSSE = model.computeCost(dataset)
println(s"Within Set Sum of Squared Errors = $WSSSE")

// Shows the result.
println("Cluster Centers: ")
// model.clusterCenters.foreach(println)