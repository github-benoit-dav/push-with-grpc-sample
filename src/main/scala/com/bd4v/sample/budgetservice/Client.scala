package com.bd4v.sample.budgetservice

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.grpc.GrpcClientSettings
import akka.stream.QueueOfferResult
import akka.stream.scaladsl.Source
import com.bd4v.sample.budgetservice.model.AdId
import org.slf4j.LoggerFactory
import com.bd4v.sample.budgetservice.proto.{BudgetServiceClient, SubscriptionRequest}

import scala.concurrent.ExecutionContext

object Client extends App {
  private val logger = LoggerFactory.getLogger(classOf[Client.type])

  implicit val sys: ActorSystem[_] = ActorSystem(Behaviors.empty, "BudgetServiceClient")
  implicit val ec: ExecutionContext = sys.executionContext

  private val client = BudgetServiceClient(GrpcClientSettings.fromConfig("budget.service"))

  private val (subscriptionQueue, subscriptionSrc) =
    Source.queue[SubscriptionRequest](1000).preMaterialize()
  private val statusSrc =
    client.subscribeToStatus(subscriptionSrc)

  private def subscribe(adId: AdId): Unit = {
    val result = subscriptionQueue.offer(SubscriptionRequest(adId.value, subscriptionToggle = true))
    assert(result == QueueOfferResult.Enqueued)
  }

  subscribe(AdId(1))
  subscribe(AdId(2))

  statusSrc.runForeach { adStatus =>
    logger.info("Ad {} has status {}", adStatus.adId, adStatus.status)
  }.onComplete(_ => sys.terminate())
}
