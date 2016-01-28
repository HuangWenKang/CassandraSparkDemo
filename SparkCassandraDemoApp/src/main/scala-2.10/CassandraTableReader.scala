import org.apache.spark.SparkContext
import com.datastax.spark.connector._

class CassandraTableReader()(implicit val sc : SparkContext) {

  //Make sure you have done the steps in this post 1st
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
  def readTestTableValues(): Unit = {
    println(s"In method : readTestTableValues")
    val rdd = sc.cassandraTable("test", "kv")
    val count = rdd.count
    val first = rdd.first()
    val sum = rdd.map(_.getInt("value")).sum
    println(s"============ RDD COUNT $count")
    println(s"============ RDD FIRST $first")
    println(s"============ RDD SUM $sum")
  }


  //Make sure you have done the steps in this post 1st
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
  def foreachTestTableValues(): Unit = {
    println(s"In method : foreachTestTableValues")
    val rdd = sc.cassandraTable("test", "kv")
    rdd.foreach(println)
  }


  //Make sure you have done the steps in this post 1st
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
  def getColumnAttributes(): Unit = {
    println(s"In method : getColumnValue")
    val rdd = sc.cassandraTable("test", "kv")
    val firstRow = rdd.first
    val colNames = firstRow.columnNames
    val colSize = firstRow.size
    val firstKey = firstRow.get[String]("key")
    val firstValue = firstRow.get[Int]("value")
    println(s"============ RDD COLUMN NAMES $colNames")
    println(s"============ RDD COLUMN SIZE $colSize")
    println(s"============ RDD FIRST KEY $firstKey")
    println(s"============ RDD FISRT $firstValue")

  }

  //Make sure you have done the steps in this post 1st
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
  def getSets(): Unit = {
    println(s"In method : getSets")
    val rdd = sc.cassandraTable("test", "users")
    val row = rdd.first()
    printIt("getSets", "List[String]", row.get[List[String]]("emails"))
    printIt("getSets", "IndexedSeq[String]", row.get[IndexedSeq[String]]("emails"))
    printIt("getSets", "Seq[String]", row.get[Seq[String]]("emails"))
    printIt("getSets", "Set[String]", row.get[Set[String]]("emails"))
  }


  //Make sure you have done the steps in this post 1st
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
  def getUDT(): Unit = {
    println(s"In method : getUDT")
    val rdd = sc.cassandraTable("test", "companies")
    val row = rdd.first()
    val address: UDTValue = row.getUDTValue("address")
    printIt("getUDT", "city", address.getString("city"))
    printIt("getUDT", "street", address.getString("street"))
    printIt("getUDT", "number", address.getString("number"))
  }


  def printIt(method : String, name : String, data : AnyRef): Unit = {
    println(s"============ $method : $name $data")
  }

}
