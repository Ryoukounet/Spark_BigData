import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import scala.util.Try
import Array._

val start = System.nanoTime
val data =sc.textFile("/home/ryouk/Documents/spark/Crimes-2001-present.csv")
val cleanData = data.map(line => {
    val splitedLine = line.split(",")
    val splittedArrayLength = splitedLine.length
    (splitedLine(splittedArrayLength-4),splitedLine(splittedArrayLength-3))
    }) //lattitude longitude
val prePreData=cleanData.filter(line =>(!line._1.isEmpty && !line._2.isEmpty 
    && Try(line._1.toDouble).isSuccess 
    && Try(line._2.toDouble).isSuccess))
val preData = prePreData.map(s => Vectors.dense(s._1.toDouble, s._2.toDouble)).cache()
val numClusters = 10 //wssse
val numIterations = 50
val clusters = KMeans.train(preData, numClusters, numIterations)
//val WSSSE = clusters.computeCost(preData) // ça aide a trouver le nombre  de clusters optimal
val assignedData = clusters.predict(preData)
val fullData = clusters.clusterCenters.zipWithIndex.map { case (x, i) => (x(0), x(1), assignedData.filter(x => x == i).count()) }

val clusterDesc = fullData.map(t => (t)).sortBy(-_._3)
val clusterAsc = fullData.map(t => (t)).sortBy(_._3)
val troisMax = clusterDesc.take(3).map(line => (line,"max"))
val troisMin =clusterAsc.take(3).map(line => (line,"min"))
val res = troisMax.union(troisMin)

sc.parallelize(res).coalesce(1).saveAsTextFile("/home/ryouk/Documents/spark/question_3_full")

val time = (System.nanoTime - start) / 1e9d
println(time + " s")