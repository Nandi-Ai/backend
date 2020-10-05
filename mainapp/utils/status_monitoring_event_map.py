from mainapp.models import Study
from mainapp.utils.monitoring.monitor_events import MonitorEvents

status_monitoring_event_map = {
    Study.VM_STOPPED: MonitorEvents.EVENT_STUDY_VM_STOPPED,
    Study.VM_ACTIVE: MonitorEvents.EVENT_STUDY_VM_STARTED,
}
