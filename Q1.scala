/* val start = System.nanoTime
val data =sc.textFile("/home/ryouk/Documents/spark/Crimes-2001-present.csv")
val mappedData =data.map(line => line.split(","))
val selectedData =mappedData.map(line=> (line(5),1))
val reducedData =selectedData.reduceByKey(_+_)
val sortedData =reducedData.sortBy(_._2)
sortedData.coalesce(1).saveAsTextFile("/home/ryouk/Documents/spark/question1")
val time = (System.nanoTime - start) / 1e9d
println(duration + " s")
*/

val start = System.nanoTime
sc.textFile("/home/ryouk/Documents/spark/Crimes-2001-present.csv").map(line => line.split(",")).map(line=> (line(5),1)).reduceByKey(_+_).sortBy(_._2).coalesce(1).saveAsTextFile("/home/ryouk/Documents/spark/question_1_full")
val time = (System.nanoTime - start) / 1e9d
println(time + " s")