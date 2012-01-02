package livingsocial.persistence.mysql
import java.sql.SQLException
import java.util.concurrent.Executors
import org.async.jdbc.SuccessCallback
import org.async.mysql.protocol.packets.OK
import org.async.mysql.MysqlConnection
import org.async.net.Multiplexer
import akka.actor.ActorSystem
import akka.actor.Props
import java.nio.channels.SocketChannel
import java.net.InetSocketAddress
import java.nio.channels.Selector
import java.nio.channels.SelectionKey
import org.async.jdbc.AsyncConnection
import java.util.concurrent.ExecutorService

abstract class Database(host: String, port: Int, user: String, password: String, database: String) {
  val multiplexer: Multiplexer
  val system = ActorSystem("Mysql")
  implicit def dispatcher = system.dispatcher
  val eventLoopService: ExecutorService

  def eventLoop = new Runnable {
    def run: Unit = {
      try {
        while (true) {
          multiplexer.select(5)
        }
      } catch {
        case e => e.printStackTrace
      }
    }
  }

  def connection = {
    val channel = openChannel()
    val key = channel.register(multiplexer.getSelector, SelectionKey.OP_CONNECT);
    val connection = createNewConnection(channel, key)
    multiplexer.registerProcessor(key, connection)

    channel.connect(new InetSocketAddress(host, port))

    val myActor = system.actorOf(Props(new LocalMysqlConnectionActor(connection)))
    new DBConnection(myActor)
  }
  
  def close() =  {    
    system.shutdown()
    eventLoopService.shutdown()
    multiplexer.close()
    true
  }

  private def createNewConnection(channel: SocketChannel, key: SelectionKey) = {
    new MysqlConnection(user, password, database, channel, key, new SuccessCallback() {
      override def onSuccess(ok: OK): Unit = {
        System.out.println("OK")
      }
      override def onError(e: SQLException): Unit = {
        e.printStackTrace();
      }
    })
  }

  private def openChannel() = {
    val channel = SocketChannel.open();
    channel.configureBlocking(false);
    channel
  }
}

object Database {
  def apply(host: String, port: Int, user: String, password: String, database: String) = new Database(host, port, user, password, database) {
    val multiplexer = new Multiplexer
    val eventLoopService = Executors.newSingleThreadExecutor()
    eventLoopService.submit(eventLoop)
  }
}