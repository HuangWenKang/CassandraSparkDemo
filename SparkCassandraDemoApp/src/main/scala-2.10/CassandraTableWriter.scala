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
      session.execute("CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")

      //create table kv
      session.execute("DROP TABLE IF EXISTS test.kv")
      session.execute("CREATE TABLE test.kv(key text PRIMARY KEY, value int)")
      session.execute("INSERT INTO test.kv(key, value) VALUES ('key1', 1)")
      session.execute("INSERT INTO test.kv(key, value) VALUES ('key2', 2)")

      //create table emails
      session.execute("DROP TABLE IF EXISTS test.users ")
      session.execute("CREATE TABLE test.users  (username text PRIMARY KEY, emails SET<text>)")
      session.execute("INSERT INTO test.users  (username, emails) " +
        "VALUES ('sacha', {'sacha@email.com', 'sacha@hotmail.com'})")

      //create address and company
      session.execute("DROP TYPE IF EXISTS test.address")
      session.execute("DROP TABLE IF EXISTS test.companies")
      session.execute("CREATE TYPE test.address (city text, street text, number int)")
      session.execute("CREATE TABLE test.companies (name text PRIMARY KEY, address FROZEN<address>)")
    }
  }




}


