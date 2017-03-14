* spark统计 pv日志平均翻页数*
```scala
//2017.3.13  平均翻页数

val inputs = sc.textFile("s3://logs.culiu.org/chuchujie_log/search_category_pv_hourly/20170306/*/part-*")
val input = inputs.map(x=>x.split("\u0001")).filter(x=>x.length==16).map(x=>(x(4),x(7),x(9).toLong,x(6)))//4:device Id  7:KeyWords (2) 9:rank  6: "SEARCH"
val tmp = input.filter(x =>x._2.split("-").length == 4 && x._4.equals("SEARCH")).map(y => ((y._1,y._2.split("-")(2)),y._3))
val tmp2 = tmp.groupByKey().map(y => (y._1,y._2.max))
 // tmp2 ((device ID,Keywords),rankmax)
val tmp3 = tmp2.map(y => (y._1._2,y._2)).reduceByKey(_ + _)
 // tmp3 (Keywords,rankmax_sum)
val tmp4 = tmp2.map(y => (y._1._2,1)).reduceByKey(_ + _)
 // tmp4 (Keywords,KeyWords_times)
val tmp5 = tmp3.join(tmp4).map(x => (x._1,((x._2._1/x._2._2).toFloat,x._2._2))).sortBy(x => x._2._2,false).take(100).map(y => s"${y._1}\t${y._2._1}\t${y._2._2}")
 // tmp5 (Keywords,Average_Pages)
val outputResult = sc.makeRDD[String](tmp5)
outputResult.repartition(1).saveAsTextFile("s3://forall/liux/test")
```
