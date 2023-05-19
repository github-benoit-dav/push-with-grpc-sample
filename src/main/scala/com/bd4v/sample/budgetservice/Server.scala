package com.bd4v.sample.budgetservice

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior, Terminated}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import com.bd4v.sample.budgetservice.actor.DispatcherActor
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import com.bd4v.sample.budgetservice.proto.BudgetServiceHandler

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object Server extends App {

  private val logger = LoggerFactory.getLogger(classOf[Server.type])

  def apply(): Behavior[NotUsed] =
    Behaviors.setup { context =>
      implicit val system: ActorSystem[Nothing] = context.system
      implicit val ec: ExecutionContextExecutor = context.executionContext

      val dispatcherRef = context.spawn(DispatcherActor(), "Dispatcher")
      context.watch(dispatcherRef)

      val service: HttpRequest => Future[HttpResponse] =
        BudgetServiceHandler(new BudgetServiceImpl(dispatcherRef))
      val httpServerInitFuture: Future[Http.ServerBinding] = Http(system)
        .newServerAt(interface = "127.0.0.1", port = 8080)
        .bind(service)
        .map(_.addToCoordinatedShutdown(hardTerminationDeadline = 10.seconds))

      httpServerInitFuture.onComplete {
        case Success(binding) =>
          val address = binding.localAddress
          logger.info("gRPC server bound to {}:{}", address.getHostString, address.getPort)
        case Failure(ex) =>
          logger.info("Failed to bind gRPC endpoint, terminating system", ex)
          system.terminate()
      }

      Behaviors.receiveSignal { case (_, Terminated(_)) =>
        logger.info(s"[Main] Dispatcher stopped")
        Behaviors.stopped
      }
    }

  try {
    val conf = ConfigFactory
      .parseString("akka.http.server.preview.enable-http2 = on")
      .withFallback(ConfigFactory.defaultApplication())
    ActorSystem(Server(), "budget-service", conf).whenTerminated.onComplete {
      case Failure(e) =>
        logger.error(s"Budget Service terminated with error: ${e.getMessage}", e)
      case Success(_) =>
        logger.info("Budget Service terminated normally")
    }(ExecutionContext.parasitic)
  } catch {
    case NonFatal(e) =>
      logger.error("Application terminated with error", e)
      System.exit(1)
  }
}