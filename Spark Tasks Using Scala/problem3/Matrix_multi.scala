import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{concat,lit}

case class MM(i:Int, j:Int, Mij:Int)
case class NN(j:Int, k:Int, Njk:Int)

val sqlContext = new SQLContext(sc)
val m = sc.textFile("/home/mqp/Desktop/M1.txt")
val n = sc.textFile("/home/mqp/Desktop/M2.txt")

val M = m.map(_.split(",")).map(p => MM(p(0).toInt, p(1).toInt, p(2).toInt)).toDF()
val N = n.map(_.split(",")).map(p => NN(p(0).toInt, p(1).toInt, p(2).toInt)).toDF()

val M_map = M.withColumn("valueM", concat(lit("M,"),col("j"),lit(","),col("Mij")))
M_map.registerTempTable("M_map")
val N_map = N.withColumn("valueN", concat(lit("N,"),col("j"),lit(","),col("Njk")))
N_map.registerTempTable("N_map")

val joinMN = sqlContext.sql("select M_map.i, N_map.k, M_map.Mij*N_map.Njk as Sik from M_map, N_map where M_map.j = N_map.j")
joinMN.registerTempTable("joinMN")

val S = sqlContext.sql("select i, k, sum(Sik) as Sik from joinMN group by i,k order by i,k")
S.registerTempTable("S")

S.show


