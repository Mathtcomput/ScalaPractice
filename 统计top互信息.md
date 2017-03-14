* 统计top互信息 *
```scala
//2017.3.14  统计top互信息 注释版

import math._
val inputs=sc.textFile("s3://logs.culiu.org/chuchujie_log/search_category_pv_hourly/20170306/*/part-*")
val temp=inputs.map(x=>x.split("\u0001")).filter(x=>x.length==16).map(x=>(x(4),x(7),x(9),x(6),x(0).toLong))
val temp2=temp.filter(x=>x._2.split("-").length==4 && x._3.equals("1") && x._4.equals("SEARCH")).map(x=>
	(x._1,x._2.split("-")(2),x._5)).map(x => (x._1, (x._2, x._3))) 
  //tmp2 (device ID,(KeyWords,time))
val temp3 = temp2.groupByKey().map(x => (x._1, x._2.toSeq.sortBy(y => y._2).map(y => y._1))).map{x =>
	val origion_list = x._2;
	val changed_list = x._2.drop(1);
	val pairs_list = origion_list.zip(changed_list);//zip 操作 （1,2,3）=> drop(1) (2,3) => zip (1,2)(2,3)
	(x._1, pairs_list.filter(y => !y._1.equals(y._2)))} 
  //tmp3 (device ID,((Keywords_1,Keywords_2),(Keywords_3,Keywords_4)))
val temp5 = temp3.flatMap(x => x._2.map(y => (y, 1))).reduceByKey(_ + _)
  //tmp5 ((Keywords_1,Keywords_2),abTimes)  
val rdd1 = temp5.map(x => (x._1._1,x._2)).reduceByKey(_ + _) 
  //rdd1 (Keywords_1,aTimes)
val rdd2 = temp5.map(x => (x._1._2,x._2)).reduceByKey(_ + _)
  //rdd2 (Keywords_2,bTimes)
val rdd3 = temp5.map(x => (x._1._1,(x._1._2,x._2)))
  //rdd3 (Keywords_1,(Keywords_2,abTimes))
val rddResult = rdd3.join(rdd1).map(y => (y._2._1._1,(y._1,y._2._1._2,y._2._2))).join(rdd2).map(z => (z._2._1._1,(z._1,z._2._1._2,z._2._1._3,z._2._2)))
  //rddResult (Keywords_1,(Keywords_2,abTimes,aTimes,bTimes))
val inputtemp = rddResult.map(x => (x._1,(x._2._1,(x._2._2/727820.toFloat)*log(x._2._2/(x._2._3*x._2._4).toFloat*727820)))).groupByKey().map(x => (x._1,x._2.toSeq.sortWith(_._2>_._2))).map(x => (x._1,(x._2(0)._1,x._2(0)._2)))
  //inputtemp (Keywords_1,(Keywords_2,Mutual_Inf_Score))
val inputttemp = inputtemp.map(y => ((y._1,y._2._1),y._2._2))
  //inputttemp ((Keywords_1,Keywords_2),Mutual_Inf_Score)
val Result_ = inputttemp.sortBy(x => x._2,false).take(100).map(y => s"${y._1._1}\t${y._1._2}\t${y._2}")
val output = sc.makeRDD[String](Result_)
output.repartition(1).saveAsTextFile("s3://forall/liux/test")
```
