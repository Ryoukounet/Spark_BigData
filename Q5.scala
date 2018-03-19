val start = System.nanoTime
val data =sc.textFile("/home/ryouk/Documents/spark/Crimes-2001-present.csv")

val selectedData =data.map(line => line.split(",")).map(line=> (line(2)))
val selectedData2=selectedData.map(line => line.split(" ")).map(line=>(line(0)))
val selectedData3=selectedData2.map(line => line.split("/")).map(line=>(line(0),1))

val reducedData =selectedData3.reduceByKey(_+_)
val sortedData =reducedData.sortBy(_._2,false)

sortedData.saveAsTextFile("/home/ryouk/Documents/spark/question_5_full")
val time = (System.nanoTime - start) / 1e9d
println(time + " s")