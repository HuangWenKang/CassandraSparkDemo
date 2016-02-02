import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext

class CassandraTableWriter()(implicit val sc : SparkContext) {

  //Make sure you have done the steps in this post 1st
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
  def writeTestTableValues(): Unit = {
    println(s"In method : writeTestTableValues")
    val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
    collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))
  }

  def initialise(): Unit =   {
    val conf = sc.getConf
    CassandraConnector(conf).withSessionDo { session =>

      //create keyspace
      session.execute("DROP KEYSPACE IF EXISTS test")
      session.execute("CREATE KEYSPACE test WITH REPLICATION = " +
        "{'class': 'SimpleStrategy', 'replication_factor': 1 }")

      //create table kv
      session.execute("DROP TABLE IF EXISTS test.kv")
      session.execute("CREATE TABLE test.kv(key text PRIMARY KEY, value int)")
      session.execute("INSERT INTO test.kv(key, value) VALUES ('key1', 1)")
      session.execute("INSERT INTO test.kv(key, value) VALUES ('key2', 2)")


      //create table words
      session.execute("DROP TABLE IF EXISTS test.words")
      session.execute("CREATE TABLE test.words(word text PRIMARY KEY, count int)")
      session.execute("INSERT INTO test.words(word, count) VALUES ('elephant', 1)")
      session.execute("INSERT INTO test.words(word, count) VALUES ('tiger', 12)")
      session.execute("INSERT INTO test.words(word, count) VALUES ('snake', 5)")


      //create table emails
      session.execute("DROP TABLE IF EXISTS test.users ")
      session.execute("CREATE TABLE test.users  (username text PRIMARY KEY, emails SET<text>)")
      session.execute("INSERT INTO test.users  (username, emails) " +
        "VALUES ('sacha', {'sacha@email.com', 'sacha@hotmail.com'})")
      session.execute("INSERT INTO test.users  (username, emails) " +
        "VALUES ('bill', {'bill23@email.com', 'billybob@hotmail.com'})")

      //create address and company
      session.execute("DROP TYPE IF EXISTS test.address")
      session.execute("DROP TABLE IF EXISTS test.companies")
      session.execute("CREATE TYPE test.address (city text, street text, number int)")
      session.execute("CREATE TABLE test.companies (name text PRIMARY KEY, address FROZEN<address>)")
      session.execute("INSERT INTO test.companies (name, address) VALUES ('company1', " +
        "{ city : 'London', street : 'broad street', number : 111 })")
      session.execute("INSERT INTO test.companies (name, address) VALUES ('company2', " +
        "{ city : 'Reading', street : 'rushmore road', number : 43 })")
    }
  }


  //See this post for more details
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/5_saving.md
  def saveCollectionOfTuples(): Unit = {
    println(s"In method : saveCollectionOfTuples")

    val collection = sc.parallelize(Seq(("cat", 30), ("fox", 40)))
    collection.saveToCassandra("test", "words", SomeColumns("word", "count"))
  }



}


