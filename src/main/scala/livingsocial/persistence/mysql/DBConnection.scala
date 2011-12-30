package livingsocial.persistence.mysql
import akka.dispatch.Future
import org.async.jdbc.ResultSet
import org.async.mysql.MysqlConnection
import org.async.net.Multiplexer
import org.async.mysql.protocol.packets.OK
import org.async.jdbc.SuccessCallback
import java.sql.SQLException
import org.async.jdbc.ResultSetCallback
import akka.actor.Actor
import akka.event.Logging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.util.Timeout
import akka.util.duration._
import org.async.jdbc.AsyncConnection
import akka.actor.Kill

case class Query(q: String)

class DBConnection(underlyingConnection: ActorRef) {
  implicit val timeout = Timeout(12 millis)
  def select(query: String): Future[ResultSet] = {
    val f = underlyingConnection ? Query(query)
    f.mapTo[ResultSet]
  }  
  
  def close() {
    underlyingConnection ! Kill
  }
}

class LocalMysqlConnectionActor(_connection: AsyncConnection) extends Actor {
  val log = Logging(context.system, this)
  def receive = {
    case Query(q) ⇒
      val st = _connection.createStatement()
      val s = sender
      st.executeQuery(q, new ResultSetCallback() {
        def onResultSet(rs: ResultSet) {
          s ! rs
        }
        def onError(e: SQLException) {
          s ! akka.actor.Status.Failure(e)
        }
      })
    case _ ⇒ log.info("received unknown message")
  }
}
