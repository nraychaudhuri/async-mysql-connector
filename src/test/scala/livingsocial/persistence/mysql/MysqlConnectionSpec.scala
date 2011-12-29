package livingsocial.persistence.mysql

import org.specs2.mutable._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.async.jdbc.ResultSet
import akka.util.duration._
import akka.util.Timeout
import java.util.concurrent.TimeUnit

@RunWith(classOf[JUnitRunner])
object MysqlConnectionSpec extends Specification {
  implicit val timeout = Timeout(20000, TimeUnit.MILLISECONDS)
  
  "DBConnection" should {
    "allow to execute select query" in {
       val connection = DBConnection("localhost",3306, "root", "","async_mysql_test")
       val future = connection.select("select * from test")
       import akka.dispatch.Await
       val rs = Await.result(future, timeout.duration).asInstanceOf[ResultSet]
       println(rs)
       while(rs.hasNext()) {
         rs.next()
        System.out.println(rs.getLong(1) + " " + rs.getString(2)
               + " " + rs.getString(3) + " " + rs.getTimestamp(4));
       }
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