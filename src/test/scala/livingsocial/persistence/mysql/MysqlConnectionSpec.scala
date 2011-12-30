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
import java.sql.Date

@RunWith(classOf[JUnitRunner])
object MysqlConnectionSpec extends Specification {
  implicit val timeout = Timeout(2000, TimeUnit.MILLISECONDS)

  "DBConnection" should {
    "allow to create database, create table, insert and query data from table" in {
      val db = Database("localhost", 3306, "root", "", "")
      val connection = db.connection
      val future = for {
        _ <- connection.executeUpdate("CREATE DATABASE IF NOT EXISTS async_mysql_test collate utf8_general_ci")
        _ <- connection.executeUpdate("USE async_mysql_test")
        _ <- connection.executeUpdate("CREATE TABLE IF NOT EXISTS test  ("
          + "id int(11) NOT NULL auto_increment,"
          + "text0 TEXT collate utf8_general_ci,"
          + "varchar0 VARCHAR(255) collate utf8_general_ci NOT NULL,"
          + "date0 DATETIME,"
          + "PRIMARY KEY  (id)"
          + ") ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;")
        _ <- connection.executeUpdate("TRUNCATE test")
        _ <- connection.executeUpdate2("INSERT INTO test SET varchar0=?,date0=?") { pstmt =>
          pstmt.setString(1, "text text text")
          pstmt.setDate(2, new Date(System.currentTimeMillis()))
        }
        r <- connection.select2("select * from test where id=?") { pstmt =>
          pstmt.setInteger(1, 1)
        }
      } yield {
        r
      }
      val rs = Await.result(future, timeout.duration).asInstanceOf[ResultSet]
      while (rs.hasNext()) {
        rs.next()
        System.out.println(rs.getLong(1) + " " + rs.getString(2)
          + " " + rs.getString(3) + " " + rs.getTimestamp(4));
        1 must beEqualTo(1)
        "text text text" must beEqualTo(rs.getString(3))
      }
      db.close()
    }

    "allow to execute select query" in {
      val db = Database("localhost", 3306, "root", "", "async_mysql_test")
      val futures = (1 to 50) map { x =>
        val connection = db.connection
        connection.select("select * from test")
      }

      futures.foreach { f =>
        val rs = Await.result(f, timeout.duration).asInstanceOf[ResultSet]
        while (rs.hasNext()) {
          rs.next()
          System.out.println(rs.getLong(1) + " " + rs.getString(2)
            + " " + rs.getString(3) + " " + rs.getTimestamp(4));
          1 must beEqualTo(1)
          "text text text" must beEqualTo(rs.getString(3))
        }
      }
      db.close()
    }
  }
}
