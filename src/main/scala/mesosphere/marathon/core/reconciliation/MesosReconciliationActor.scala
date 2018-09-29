package mesosphere.marathon
package core.reconciliation

import akka.Done
import akka.pattern.ask
import akka.actor.{Actor, Cancellable}
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.MarathonSchedulerActor.{ReconcileImplicitly, ReconcileTasks, TasksReconciled}
import mesosphere.marathon.TaskStatusCollector
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.reconciliation.MesosReconciliationActor.{InstancesToReconcile, ReconciliationFinished, TriggerReconciliation}
import mesosphere.marathon.core.task.termination.{KillReason, KillService}
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.storage.repository.GroupRepository
import org.apache.mesos.Protos.Status
import org.apache.mesos.{Protos, SchedulerDriver}

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

class MesosReconciliationActor(
    instanceTracker: InstanceTracker,
    groupRepository: GroupRepository,
    killService: KillService,
    scaleAppsInitialDelay: FiniteDuration,
    schedulerService: MarathonSchedulerService,
    scaleAppsInterval: FiniteDuration) extends Actor with StrictLogging {
  private[this] var periodicOffers: Option[Cancellable] = None
  var activeReconciliation: Option[Future[Done]] = None

  override def preStart(): Unit = {
    import context.dispatcher

    super.preStart()

    periodicOffers = Some(context.system.scheduler.schedule(scaleAppsInitialDelay, scaleAppsInterval, self, TriggerReconciliation))
  }

  override def postStop(): Unit = {
    periodicOffers.foreach(_.cancel())
  }

  def triggerReconciliation(): Future[Done] = async {
    val tasks = await(tasksToReconcile())
    tasks.toKill.foreach(killService.killInstance(_, KillReason.Orphaned))
    await(schedulerActor ? ReconcileTasks(tasks.toReconcile))
    await(schedulerActor ? ReconcileImplicitly)
    self ! ReconciliationFinished
    Done
  }

  override def receive: Receive = {
    case TriggerReconciliation =>
      import akka.pattern.pipe
      import akka.pattern.ask

      val reconcileFuture = async {
        activeReconciliation match {
          case None =>
            logger.info("initiate task reconciliation")
            val newFuture = triggerReconciliation()
            newFuture.failed.foreach {
              case NonFatal(e) => logger.error("error while reconciling tasks", e)
            }
            activeReconciliation = Some(newFuture)
          case Some(active) =>
            logger.info("task reconciliation still active, reusing result")
            active
        }
      }
      reconcileFuture.map(_ => TasksReconciled).pipeTo(sender())
    case ReconciliationFinished =>
      logger.info("task reconciliation has finished")
      activeReconciliation = None
  }

  /**
    * Make sure all runSpecs are running the configured amount of tasks.
    *
    * Should be called some time after the framework re-registers,
    * to give Mesos enough time to deliver task updates.
    *
    */
  private def tasksToReconcile(): Future[InstancesToReconcile] = async {
    val root = await(groupRepository.root())

    val runSpecIds = root.transitiveRunSpecIds.toSet
    val instances = await(instanceTracker.instancesBySpec())

    val knownTaskStatuses = runSpecIds.flatMap { runSpecId =>
      TaskStatusCollector.collectTaskStatusFor(instances.specInstances(runSpecId))
    }

    val orphanedInstances = (instances.allSpecIdsWithInstances -- runSpecIds).flatMap { unknownId =>
      logger.warn(
        s"RunSpec $unknownId exists in InstanceTracker, but not store. " +
          "The run spec was likely terminated. Will now expunge."
      )
      instances.specInstances(unknownId)
    }
    InstancesToReconcile(orphanedInstances, knownTaskStatuses)
  }
}

object MesosReconciliationActor {
  object TriggerReconciliation
  object ReconciliationFinished
  object TasksReconciled

  case class InstancesToReconcile(toKill: Set[Instance], toReconcile: Set[Protos.TaskStatus])
}
