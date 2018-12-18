import org.apache.spark.sql.SQLContext
case class Transactions(TransID:Int, CustID:Int, TransT:Float, TransI:Int, TransD:String)
val sqlContext = new SQLContext(sc)

val textFileF = sc.textFile("/home/mqp/Desktop/Transaction.txt")
val Transaction = textFileF.map(_.split(",")).map(p => Transactions(p(0).toInt, p(1).toInt, p(2).toFloat, p(3).toInt, p(4))).toDF()
Transaction.createOrReplaceTempView("Trans")
val T = sqlContext.sql("select * from Trans")
T.registerTempTable("T")
T.show()

val T1 = sqlContext.sql("select * from T where TransT >= 200")
T1.registerTempTable("T1")
T1.show()

val T2 = sqlContext.sql("select TransI as item_count, sum(TransT) as sum, avg(TransT) as avg, min(TransT) as min, max(TransT) as max from T1 group by TransI")
T2.registerTempTable("T2")
T2.show()

val T3 = sqlContext.sql("select CustID, count(*) as Trans_count from T1 group by CustID")
T3.registerTempTable("T3")
T3.show()

val T4 = sqlContext.sql("select * from T where TransT >= 600")
T4.registerTempTable("T4")
T4.show()

val T5 = sqlContext.sql("select CustID, count(*) as Trans_count from T4 group by CustID")
T5.registerTempTable("T5")
T5.show()

val T6 = sqlContext.sql("select T5.CustID from T5, T3 where T5.CustID = T3.CustID and T5.Trans_count*3 < T3.Trans_count group by T5.CustID")
T6.registerTempTable("T6")
T6.show()

	
