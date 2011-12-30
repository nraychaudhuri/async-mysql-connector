package livingsocial.persistence.mysql
import java.sql.SQLException
import java.util.concurrent.Executors

import org.async.jdbc.SuccessCallback
import org.async.mysql.protocol.packets.OK
import org.async.mysql.MysqlConnection
import org.async.net.Multiplexer

import akka.actor.ActorSystem
import akka.actor.Props

abstract class Database(host: String, port: Int, user: String, password: String, database: String) {
  val multiplexer: Multiplexer
  val system = ActorSystem("Mysql")
  implicit def dispatcher = system.dispatcher

  def eventLoop = new Runnable {
    def run: Unit = {
      while (true) {
        multiplexer.select(5)
      }
    }
  }

  def connection = {
    val _connection = new MysqlConnection(host, port, user, password, database, multiplexer.getSelector, new SuccessCallback() {
      override def onSuccess(ok: OK): Unit = {
        System.out.println("OK")
      }
      override def onError(e: SQLException): Unit = {
        e.printStackTrace();
      }
    })
    val myActor = system.actorOf(Props(new LocalMysqlConnectionActor(_connection)), name = "myactor")
    new DBConnection(myActor)
  }

  def close() = system.shutdown()
}

object Database {
  def apply(host: String, port: Int, user: String, password: String, database: String) = new Database(host, port, user, password, database) {
    val multiplexer = new Multiplexer
    Executors.newSingleThreadExecutor().submit(eventLoop)
  }
}