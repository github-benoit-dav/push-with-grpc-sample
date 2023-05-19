package com.bd4v.sample.budgetservice

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.ActorRef
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.bd4v.sample.budgetservice.model.{AdId, ConnectionId}
import org.slf4j.LoggerFactory
import com.bd4v.sample.budgetservice.proto.{AdStatus, BudgetService, SubscriptionRequest}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

object BudgetServiceImpl {
  private val connectionIdCounter = new AtomicInteger(0)
  private val logger = LoggerFactory.getLogger(classOf[BudgetServiceImpl])

  def apply(system: ActorSystem): Unit = {

  }
}
import BudgetServiceImpl._

class BudgetServiceImpl(
                         dispatcherRef: ActorRef[DispatcherActor.Command]
                       )(implicit mat: Materializer) extends BudgetService {

  private implicit val ec: ExecutionContextExecutor = mat.executionContext

  override def subscribeToStatus(
                                  inSrc: Source[SubscriptionRequest, NotUsed]): Source[AdStatus, NotUsed] = {
    val (outBoundQueue, outSrc) =
      Source.queue[AdStatus](1000).preMaterialize()
    val id = ConnectionId(connectionIdCounter.incrementAndGet())
    dispatcherRef ! DispatcherActor.Connect(id, outBoundQueue)
    inSrc
      .runForeach { request =>
        if (request.subscriptionToggle) {
          logger.info("{} subscribed to {}", id.value, request.adId)
          dispatcherRef ! DispatcherActor.Subscribe(id, AdId(request.adId))
        } else {
          logger.info("{} unsubscribed to {}", id.value, request.adId)
          dispatcherRef ! DispatcherActor.Unsubscribe(id, AdId(request.adId))
        }

      }.onComplete {
      case Success(_) =>
        logger.info("{} connection closed normally", id.value)
        dispatcherRef ! DispatcherActor.Disconnect(id)
      case Failure(e) =>
        logger.info("{} connection closed with {}", id.value, e.getMessage)
        dispatcherRef ! DispatcherActor.Disconnect(id)
    }
    logger.info("{} connected", id.value)
    outSrc.map { adStatus =>
      logger.info("{} receives {} -> {}", id.value, adStatus.adId, adStatus.status)
      adStatus
    }
  }
}
