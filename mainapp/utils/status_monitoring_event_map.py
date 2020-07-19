from mainapp.models import Study
from mainapp.utils.elasticsearch_service import MonitorEvents

status_monitoring_event_map = {
    Study.VM_STOPPING: MonitorEvents.EVENT_STUDY_VM_STOPPING,
    Study.VM_STOPPED: MonitorEvents.EVENT_STUDY_VM_STOPPED,
    Study.VM_STARTING: MonitorEvents.EVENT_STUDY_VM_STARTING,
    Study.VM_ACTIVE: MonitorEvents.EVENT_STUDY_VM_STARTED,
}
