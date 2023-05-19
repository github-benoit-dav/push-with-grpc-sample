package com.bd4v.sample.budgetservice

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import com.bd4v.sample.budgetservice.model.AdId
import com.bd4v.sample.budgetservice.proto.BudgetStatus

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object BudgetStatusComputerActor {

  sealed trait Command

  final case object Refresh extends Command

  final case class RefreshResult(status: BudgetStatus) extends Command

  final case class State(status: Option[BudgetStatus])

  def apply(
             adId: AdId,
             dispatcher: ActorRef[DispatcherActor.Command]
           ): Behavior[Command] =
    Behaviors.withTimers { timers =>
      // Force the first compute to take place immediately
      timers.startSingleTimer(Refresh, 0.second)
      new BudgetStatusComputerActor(
        adId,
        timers,
        dispatcher
      ).behavior(State(None))
    }

  private class BudgetStatusComputerActor(
                                           adId: AdId,
                                           timers: TimerScheduler[Command],
                                           dispatcherRef: ActorRef[DispatcherActor.Command]
                                         ) {

    def behavior(state: State): Behavior[Command] =
      Behaviors
        .receive[Command] { (context, message) =>
          message match {
            case Refresh =>
              timers.startSingleTimer(Refresh, 1.second)
              context.pipeToSelf(computeBudget()) {
                case Success(status) => RefreshResult(status)
                case Failure(_) => RefreshResult(BudgetStatus.BudgetError)
              }
              Behaviors.same
            case RefreshResult(status: BudgetStatus) =>
              behavior(publishAndUpdateStatuses(state, status))
          }
        }

    private def computeBudget(): Future[BudgetStatus] = {
      //Fake implementation
      Future.successful(
        if (math.random() < 0.2) BudgetStatus.BudgetExhausted
        else BudgetStatus.BudgetAvailable
      )
    }

    private def publishAndUpdateStatuses(state: State, status: BudgetStatus): State =
      if (!state.status.contains(status)) {
        dispatcherRef ! DispatcherActor.NewStatus(adId, status)
        state.copy(status = Some(status))
      } else
        state
  }

}
