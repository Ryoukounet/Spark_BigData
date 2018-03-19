

val start = System.nanoTime
sc.textFile("/home/ryouk/Documents/spark/Crimes-2001-present.csv").map(line => line.split(",")).map(line=> (line(5),1)).reduceByKey(_+_).sortBy(_._2).coalesce(1).saveAsTextFile("/home/ryouk/Documents/spark/question_1_full")
val time = (System.nanoTime - start) / 1e9d
println(time + " s")
