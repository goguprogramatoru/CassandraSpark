import com.datastax.driver.core.Cluster
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.sys.process._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext

object Boot {

	case class User(userName:String, password:String, winningTeam:Option[String])
	case class Data(col1:Int, col2:Int, col3:Int, col4:Int, col5:Int, col6:Int, col7:Int, col8:Int, col9:Int, col10:Int)
	def main (args: Array[String]) {
		//this.fillCassandra()
		this.test1()
	}

	def fillCassandra() = {
		val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
		val session = cluster.connect("spark")

		session.execute("DROP TABLE IF EXISTS data")

		session.execute(
			"""
			  |CREATE TABLE data
			  |(
			  |		col1 INT PRIMARY KEY,
			  |  	col2 INT,
			  |   	col3 INT,
			  |    	col4 INT,
			  |     col5 INT,
			  |     col6 INT,
			  |     col7 INT,
			  |     col8 INT,
			  |     col9 INT,
			  |     col10 INT
			  |);
			""".stripMargin
		)

		(1 to 1000000).par.foreach(nb => {
			session.execute(
				"""
				  |INSERT INTO data (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10)
				  |VALUES (?,?,?,?,?,?,?,?,?,?)
				""".stripMargin,
				nb:java.lang.Integer,
				scala.util.Random.nextInt(100):java.lang.Integer,
				scala.util.Random.nextInt(100):java.lang.Integer,
				scala.util.Random.nextInt(100):java.lang.Integer,
				scala.util.Random.nextInt(100):java.lang.Integer,
				scala.util.Random.nextInt(100):java.lang.Integer,
				scala.util.Random.nextInt(100):java.lang.Integer,
				scala.util.Random.nextInt(100):java.lang.Integer,
				scala.util.Random.nextInt(100):java.lang.Integer,
				scala.util.Random.nextInt(100):java.lang.Integer
			)
		})
		session.close()
		cluster.close()
		println("done!")
	}

	def test1() = {
		val conf = new SparkConf(true)
				.set("spark.cassandra.connection.host", "127.0.0.1")
				.setMaster("local[4]")
				.setAppName("cassandraSpark")

		/** Connect to the Spark cluster: */
		lazy val sc:SparkContext = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)

		val t1 = System.currentTimeMillis()

		val data:RDD[User]= sc.cassandraTable[User]("euro2016","users").toJavaRDD()
		import sqlContext.implicits._
		data.toDF().registerTempTable("bla")
		sqlContext.sql("SELECT * FROM bla").show(100)

		val t2 = System.currentTimeMillis()

		println("Took: "+(t2-t1))

		val data2:DataFrame = sc.cassandraTable[Data]("spark","data").toDF()
		data2.toDF().registerTempTable("data")
		sqlContext.sql("SELECT col2, SUM(col3) FROM data GROUP BY col2").show(100)

		val t3 = System.currentTimeMillis()
		println("Took: "+(t3-t2))

		val cc = new CassandraSQLContext(sc)
		cc.sql("SELECT col2, SUM(col3) FROM spark.data GROUP BY col2").show(100) //faster

		val t4 = System.currentTimeMillis()
		println("Took: "+(t4-t3))
	}
}
