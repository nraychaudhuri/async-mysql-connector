package livingsocial.persistence.mysql

import org.specs2.mutable._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.async.jdbc.ResultSet
import akka.util.duration._
import akka.util.Timeout
import java.util.concurrent.TimeUnit
import akka.dispatch.Await
import org.async.net.Multiplexer

@RunWith(classOf[JUnitRunner])
object MysqlConnectionSpec extends Specification {
  implicit val timeout = Timeout(20000, TimeUnit.MILLISECONDS)

  "DBConnection" should {
    "allow to execute select query" in {
      val db = Database("localhost", 3306, "root", "", "async_mysql_test")
      //(1 to 10).foreach { x =>
        val connection = db.connection
        val future = connection.select("select * from test")
        val rs = Await.result(future, timeout.duration).asInstanceOf[ResultSet]
        while (rs.hasNext()) {
          rs.next()
          System.out.println(rs.getLong(1) + " " + rs.getString(2)
            + " " + rs.getString(3) + " " + rs.getTimestamp(4));
        }
      //}
      db.close()
      1 must beEqualTo(1)
    }
    "start with 'Hello'" in {
      "Hello world" must startWith("Hello")
    }
    "end with 'world'" in {
      "Hello world" must endWith("world")
    }
  }
}