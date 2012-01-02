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
import org.async.jdbc.PreparedStatement
import org.async.jdbc.PreparedQuery
import akka.actor.PoisonPill

case class Query(q: String)
case class Query2(q: String, f: PreparedStatement => Unit)
case class Update(q: String)
case class Update2(q: String, f: PreparedStatement => Unit)

class DBConnection(underlyingConnection: ActorRef) {
  implicit val timeout = Timeout(12 millis)

  def select(query: String): Future[ResultSet] = {
    val f = underlyingConnection ? Query(query)
    f.mapTo[ResultSet]
  }

  def select2(query: String)(f: PreparedStatement => Unit): Future[ResultSet] = {
    val future = underlyingConnection ? Query2(query, f)
    future.mapTo[ResultSet]
  }

  def executeUpdate(query: String): Future[OK] = {
    val f = underlyingConnection ? Update(query)
    f.mapTo[OK]
  }

  def executeUpdate2(query: String)(f: PreparedStatement => Unit): Future[OK] = {
    val future = underlyingConnection ? Update2(query, f)
    future.mapTo[OK]
  }

  def close() {
    underlyingConnection ! PoisonPill
  }
}

class LocalMysqlConnectionActor(_connection: AsyncConnection) extends Actor {
  val log = Logging(context.system, this)
  def receive = {
    case Query(q) ⇒
      val st = _connection.createStatement()
      st.executeQuery(q, resultSetCallback(sender))
    case Query2(q, callback) ⇒
      val st = _connection.prepareStatement(q)
      st.executeQuery(new PreparedQuery() {
        override def query(pstmt: PreparedStatement) {
          callback(pstmt)
        }
      }, resultSetCallback(sender))
    case Update(q) ⇒
      val st = _connection.createStatement()
      st.executeUpdate(q, successCallback(sender))
    case Update2(q, callback) ⇒
      val st = _connection.prepareStatement(q)
      st.executeUpdate(new PreparedQuery() {
        override def query(pstmt: PreparedStatement) {
          callback(pstmt)
        }
      }, successCallback(sender))

    case _ ⇒ log.info("received unknown message")
  }

  private def resultSetCallback(s: ActorRef) = new ResultSetCallback() {
    def onResultSet(rs: ResultSet) {
      s ! rs
    }
    def onError(e: SQLException) {
      s ! akka.actor.Status.Failure(e)
    }
  }
  private def successCallback(s: ActorRef) = new SuccessCallback() {
    def onSuccess(ok: OK) {
      s ! ok
    }
    def onError(e: SQLException) {
      s ! akka.actor.Status.Failure(e)
    }
  }
}
