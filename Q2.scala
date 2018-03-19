

val start = System.nanoTime
val data =sc.textFile("/home/ryouk/Documents/spark/Crimes-2001-present.csv")
val selectedData = data.map(line => line.split(",")).map(line=> line(2))
val afternoon =selectedData.filter(line => line.contains("PM")).map(line => line.split(" ")).map(line => line(1).split(":")).map(line => line(0).toInt+12)
val morning =selectedData.filter(line => line.contains("AM")).map(line => line.split(" ")).map(line => line(1).split(":")).map(line => line(0).toInt)
val hourData =morning.union(afternoon)
val data0_4 =hourData.filter(line => (line < 5)).map(line =>("0_4",1)).reduceByKey(_+_)
val data4_8=hourData.filter(line => (line < 9 & line>4)).map(line=>("4_8",1)).reduceByKey(_+_)
val data8_12=hourData.filter(line => (line < 13 & line>8)).map(line=>("8_12",1)).reduceByKey(_+_)
val data12_16=hourData.filter(line => (line < 17 & line>12)).map(line=>("12_16",1)).reduceByKey(_+_)
val data16_20=hourData.filter(line => (line < 21 & line>16)).map(line=>("16_20",1)).reduceByKey(_+_)
val data20_24=hourData.filter(line => (line < 25 & line>20)).map(line=>("20_24",1)).reduceByKey(_+_)
val data_all=data0_4.union(data4_8).union(data8_12).union(data12_16).union(data16_20).union(data20_24)


data_all.coalesce(1).saveAsTextFile("/home/ryouk/Documents/spark/question_2_full")
val time = (System.nanoTime - start) / 1e9d
println(time + " s")
