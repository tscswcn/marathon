package mesosphere.marathon
package scheduling

import akka.Done
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.launcher.OfferProcessor
import mesosphere.marathon.core.task.termination.KillReason
import mesosphere.marathon.state.PathId
import org.apache.mesos.Protos

import scala.concurrent.{ExecutionContext, Future}

trait Scheduler extends OfferProcessor {

  //def create(runSpec: RunSpec): Future[Instance.Id]

  /**
    * Retrieve all instances for a specific run spec.
    *
    * @param runSpecId The path id of the run spec.
    * @param ec The execution context for the future.
    * @return A future list of all instances of the run spec.
    */
  def getInstances(runSpecId: PathId)(implicit ec: ExecutionContext): Future[Seq[Instance]]

  /**
    * Retrieve instance for instance id.
    *
    * @param instanceId id of the instance to retreive.
    * @param ec The execution context for the future.
    * @return A future optional instance.
    */
  def getInstance(instanceId: Instance.Id)(implicit ec: ExecutionContext): Future[Option[Instance]]

  /**
    * Run all instances with given ids.
    *
    * This method is idempotent.
    *
    * @param instances
    * @param ec
    * @return
    */
  def run(instances: Seq[Instance])(implicit ec: ExecutionContext): Future[Done]

  /**
    * Stop instances with give ids but keep them in store.
    *
    * This method is idempotent.
    *
    * @param instances The instances that should be stopped.
    * @param ec
    * @return Done when successful.
    */
  def stop(instances: Seq[Instance], killReason: KillReason)(implicit ec: ExecutionContext): Future[Done]
  def stop(instance: Instance, killReason: KillReason)(implicit ec: ExecutionContext): Future[Done] = stop(Seq(instance), killReason)

  /**
    * Stop and remove instances with given ids. This will also free all reservations.
    *
    * This method is idempotent.
    *
    * @param instances The instances that should be decommissioned.
    * @param ec
    * @return Done when successful.
    */
  def decommission(instances: Seq[Instance], killReason: KillReason)(implicit ec: ExecutionContext): Future[Done]
  def decommission(instance: Instance, killReason: KillReason)(implicit ec: ExecutionContext): Future[Done] = decommission(Seq(instance), killReason)

  /**
    * Handle a Mesos offer, e.g. free reservations or match an offer to launch instances.
    *
    * @param offer the offer to match
    * @return the future indicating when the processing of the offer has finished and if there were any errors
    */
  def processOffer(offer: Protos.Offer): Future[Done]

  def processMesosUpdate(status: Protos.TaskStatus)(implicit ec: ExecutionContext): Future[Done]
}
