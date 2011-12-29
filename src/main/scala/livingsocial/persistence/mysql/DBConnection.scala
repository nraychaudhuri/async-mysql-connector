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

case class Query(q: String)

class DBConnection(underlyingConnection: ActorRef) {
  implicit val timeout = Timeout(12 millis)
  def select(query: String): Future[ResultSet] = {
    val f = underlyingConnection ? Query(query)
    f.map(x => {println("sdgsgfsgf " + x); x.asInstanceOf[ResultSet]})
  }  
}

class LocalMysqlConnectionActor(host: String, port: Int, user: String, password: String, database: String) extends Actor {
  val log = Logging(context.system, this)
  private var _connection: AsyncConnection = null
  val mpx = new Multiplexer()
  implicit def dispatcher = ActorSystem().dispatcher
  
  private def openSelector = {
    while (true) {
      mpx.select();
    }
  }
  override def preStart = {
    _connection = new MysqlConnection(host, port, user, password, database, mpx.getSelector, new SuccessCallback() {
      override def onSuccess(ok: OK): Unit = {
        System.out.println("OK")
      }
      override def onError(e: SQLException): Unit = {
        println(">>>>>>> got an exception " + e)
        e.printStackTrace();
      }
    })
    Future(openSelector)
  }

  def receive = {
    case Query(q) ⇒
      println(">>> got the query " + q)
      val st = _connection.createStatement()
      val s = sender
      st.executeQuery(q, new ResultSetCallback() {
        def onResultSet(rs: ResultSet) {
          println(">>>>> got the result " + s)
          
          s ! rs
        }
        def onError(e: SQLException) {
          s ! akka.actor.Status.Failure(e)
        }
      })
    case _ ⇒ log.info("received unknown message")
  }
}

object DBConnection {
  def apply(host: String, port: Int, user: String, password: String, database: String) = {
    val system = ActorSystem("MySystem")
    val myActor = system.actorOf(Props(new LocalMysqlConnectionActor(host, port, user, password, database)), name = "myactor")
    new DBConnection(myActor)
  }
}