package mesosphere.marathon
package scheduling

import mesosphere.marathon.core.launcher.OfferProcessor
import mesosphere.marathon.core.task.termination.KillService
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.core.task.update.TaskStatusUpdateProcessor

class SchedulingModule(
    offerProcessor: OfferProcessor,
    instanceTracker: InstanceTracker,
    statusUpdateProcessor: TaskStatusUpdateProcessor,
    killService: KillService) {

  lazy val scheduler = LegacyScheduler(offerProcessor, instanceTracker, statusUpdateProcessor, killService)
}
