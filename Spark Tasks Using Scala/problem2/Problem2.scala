import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

def xindex(x:Int):Int={
    (x-1)/20+1;
}

def yindex(y:Int):Int={
    500-y/20-1;
}

def Neighbors(x:Int):List[Int]={
    if (x == 1)
        List(2,501,502)
    else if (x == 500)
        List(499,999,1000)
    else if (x == 249501)
        List(249001,249002,249502)
    else if (x ==250000)
        List(249499, 249500, 249999)
    else if ( x > 1 & x < 500)
        List(x-1, x+1, x+500, x+500-1, x+500+1)
    else if ( x > 249501 & x < 250000)
        List(x-1, x+1, x-500, x-500-1, x-500+1)
    else if (x % 500 == 1)
        List(x+500, x+500+1, x+1, x-500, x-500+1)
    else if (x % 500 == 0)
        List(x+500, x+500-1, x-1, x-500, x-500-1)
    else
        List(x-500-1, x-500, x-500+1, x-1, x+1, x+500-1, x+500, x+500+1)
}

val input = sc.textFile("Points")
val pairs = input.map(x => (xindex(x.split(",")(0).toInt))+(yindex(x.split(",")(1).toInt)*500))
val sortedK = pairs.map(p=>(p,1)).reduceByKey((x,y)=>x+y).sortByKey()
val pairedRDD = sc.broadcast(sortedK.collectAsMap)
val sortedV = sortedK.keys.map(x =>(x, x)).flatMapValues(x=>Neighbors(x)).map(_.swap).join(sortedK).values.sortByKey()

val r1 = sortedV.map(x=>(x._1,x._2,1)) 
val r2 = sortedV.combineByKey((v) => (v, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) ).map{ case (key, value) => (key, value._1 / value._2.toFloat) } 
val r3 = r2.sortByKey()
val r3key = sc.broadcast(r3.collectAsMap)

def index(id:Int):Double={
    return pairedRDD.value(id)/r3key.value(id)
}
//Result of step2
val gridcells = r3.map(x=>(x._1,index(x._1))).sortBy(-_._2)
gridcells.take(50)

val result = sc.broadcast(gridcells.collectAsMap)
def topN(x:Int): (List[Int], List[Double]) = {
    val n=Neighbors(x)
    var id:List[Int] = List()
    var index:List[Double] = List()
    for (y <- n) {id = id:::List(y)
        index =index:::List(result.value(y))
        }
        return (id,index)}
        
//Result of step3
val neighbors=gridcells.map(x=>(x._1,topN(x._1)))
neighbors.take(50)