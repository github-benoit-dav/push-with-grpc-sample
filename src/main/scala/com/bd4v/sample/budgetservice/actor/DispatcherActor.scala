package com.bd4v.sample.budgetservice.actor

import akka.actor.typed._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.stream.BoundedSourceQueue
import com.bd4v.sample.budgetservice.actor
import com.bd4v.sample.budgetservice.model.{AdId, ConnectionId}
import com.bd4v.sample.budgetservice.proto.{AdStatus, BudgetStatus}

object DispatcherActor {
  sealed trait Command

  final case class Connect(id: ConnectionId, queue: BoundedSourceQueue[AdStatus]) extends Command

  final case class Disconnect(id: ConnectionId) extends Command

  final case class Subscribe(id: ConnectionId, adId: AdId) extends Command

  final case class Unsubscribe(id: ConnectionId, adId: AdId) extends Command

  final case class NewStatus(adId: AdId, status: BudgetStatus) extends Command

  type Computer = BudgetStatusComputerActor.Command

  def apply(): Behavior[Command] =
    dispatcherBehavior(
      DispatcherState(Map.empty, Map.empty)
    )

  private def dispatcherBehavior(state: DispatcherState): Behavior[Command] =
    Behaviors
      .receive[Command] { (context, message) =>
        val newState = message match {
          case Connect(id, queue) =>
            state.addConnection(id, queue)

          case Disconnect(id) =>
            state
              .removeConnection(id)
              .disposeComputersIfNeeded(disposeComputer(context, _))

          case Subscribe(id, adId) =>
            state
              .subscribe(id, adId, () => spawnComputer(adId, context))
              .sendSnapshot(id, adId)

          case Unsubscribe(id, adId) =>
            state
              .unsubscribe(id, adId)
              .disposeComputerIfNeeded(adId, disposeComputer(context, _))

          case NewStatus(adId, status) =>
            state
              .updateStatuses(adId, status)
              .sendSnapshots(adId)
        }
        dispatcherBehavior(newState)
      }
      .receiveSignal {
        case (_, Terminated(_)) => Behaviors.same[Command]
      }

  private var computerIdCounter = 1

  private def spawnComputer(
                             adId: AdId,
                             context: ActorContext[Command]
                           ): ActorRef[Computer] = {
    val name = s"computer-${adId.value}-$computerIdCounter"
    computerIdCounter += 1
    val ref = context.spawn(
      actor.BudgetStatusComputerActor(adId, context.self),
      name
    )
    context.watch(ref)
    ref
  }

  private def disposeComputer(
                               context: ActorContext[Command],
                               computerRef: ActorRef[Computer]
                             ): Unit =
    context.stop(computerRef)

  final case class DispatcherState(
                                    connections: Map[ConnectionId, BoundedSourceQueue[AdStatus]],
                                    computers: Map[AdId, ComputerState],
                                  ) {
    def addConnection(
                       connectionId: ConnectionId,
                       queue: BoundedSourceQueue[AdStatus]
                     ): DispatcherState =
      copy(connections.updated(connectionId, queue))

    def removeConnection(connectionId: ConnectionId): DispatcherState =
      copy(connections.removed(connectionId))
        .copy(computers = computers.view.mapValues(_.unsubscribe(connectionId)).toMap)

    def subscribe(
                   connectionId: ConnectionId,
                   adId: AdId,
                   spawnComputer: () => ActorRef[Computer]
                 ): DispatcherState = {
      val computerState = computers
        .getOrElse(adId, ComputerState(adId, spawnComputer()))
        .subscribe(connectionId)
      copy(computers = computers.updated(adId, computerState))
    }

    def unsubscribe(id: ConnectionId, adId: AdId): DispatcherState =
      copy(computers = computers.updatedWith(adId)(_.map(_.unsubscribe(id))))

    def disposeComputerIfNeeded(
                                 adId: AdId,
                                 disposeActor: ActorRef[Computer] => Unit
                               ): DispatcherState =
      if (computers.get(adId).exists(_.isEmpty)) {
        computers.get(adId).foreach(computer => disposeActor(computer.actorRef))
        copy(computers = computers.removed(adId))
      } else
        this

    def disposeComputersIfNeeded(
                                  disposeActor: ActorRef[Computer] => Unit
                                ): DispatcherState =
      computers.keys.foldLeft(this)((state, adId) =>
        state.disposeComputerIfNeeded(adId, disposeActor)
      )

    def sendSnapshot(id: ConnectionId, adId: AdId): DispatcherState = {
      val _ = for {
        computerState <- computers.get(adId)
        status <- computerState.latestStatus
        queue <- connections.get(id)
      } yield queue.offer(AdStatus(adId.value, status))
      this
    }

    def sendSnapshots(adId: AdId): DispatcherState = {
      computers
        .get(adId)
        .foreach { computerState =>
          computerState.subscriptions.foreach {
            sendSnapshot(_, adId)
          }
        }
      this
    }

    def updateStatuses(
                        adId: AdId,
                        status: BudgetStatus
                      ): DispatcherState =
      copy(computers = computers.updatedWith(adId)(_.map(_.updateStatus(status))))

  }

  final case class ComputerState(
                                  adId: AdId,
                                  subscriptions: Set[ConnectionId],
                                  latestStatus: Option[BudgetStatus],
                                  actorRef: ActorRef[Computer]
                                ) {
    def subscribe(connectionId: ConnectionId): ComputerState =
      copy(subscriptions = subscriptions + connectionId)

    def unsubscribe(connectionId: ConnectionId): ComputerState =
      copy(subscriptions = subscriptions - connectionId)

    def updateStatus(statuses: BudgetStatus): ComputerState =
      copy(latestStatus = Some(statuses))

    def isEmpty: Boolean =
      subscriptions.isEmpty
  }

  object ComputerState {
    def apply(
               adId: AdId,
               actorRef: ActorRef[Computer]
             ): ComputerState =
      ComputerState(adId, Set.empty, None, actorRef)
  }
}
