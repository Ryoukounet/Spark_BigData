val start = System.nanoTime
val data =sc.textFile("/home/ryouk/Documents/spark/Crimes-2001-present.csv")
val dataCol = data.map(line => {
    val splitedLine = line.split(",")
    val len = splitedLine.length
    (splitedLine(len-15),splitedLine(len-4),splitedLine(len-3))
    })

val res = dataCol.filter(t => (t._1.equals("true")||t._1.equals("false"))).map(line => (line,1)).reduceByKey(_+_)
res.coalesce(1).saveAsTextFile("/home/ryouk/Documents/spark/question_4_full")
val time = (System.nanoTime - start) / 1e9d
println(time + " s")