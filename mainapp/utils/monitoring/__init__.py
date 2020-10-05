from .monitor_events import MonitorEvents

# study activity
from .study_activity.request_notebook import RequestNotebook
from .study_activity.notebook_load_fail import NotebookLoadFail
from .study_activity.notebook_ready import NotebookReady
from .study_activity.study_vm_stop_request import StudyVMStopRequest
from .study_activity.study_vm_start_request import StudyVMStartRequest
from .study_activity.study_vm_started import StudyVMStarted
from .study_activity.study_vm_stopped import StudyVMStopped
from .study_activity.study_add_dataset import StudyAddDataset
from .study_activity.study_remove_dataset import StudyRemoveDataset
from .study_activity.study_created import StudyCreated
from .study_activity.study_deleted import StudyDeleted

# user activity
from .user_activity.user_login import UserLogin
from .user_activity.user_logout import UserLogout
from .user_activity.user_idle import UserIdle
from .user_activity.user_active import UserActive

# dataset activity
from .dataset_activity.dataset_add_user import DatasetAddUser
from .dataset_activity.dataset_remove_user import DatasetRemoveUser
from .dataset_activity.dataset_created import DatasetCreated
from .dataset_activity.dataset_deleted import DatasetDeleted
from .dataset_activity.dataset_add_datasource import DatasetAddDatasource
from .dataset_activity.dataset_remove_datasource import DatasetRemoveDatasource


map_events = {
    MonitorEvents.EVENT_REQUEST_NOTEBOOK.value: RequestNotebook,
    MonitorEvents.EVENT_NOTEBOOK_LOAD_FAIL.value: NotebookLoadFail,
    MonitorEvents.EVENT_NOTEBOOK_READY.value: NotebookReady,
    MonitorEvents.EVENT_STUDY_VM_START_REQUEST.value: StudyVMStartRequest,
    MonitorEvents.EVENT_STUDY_VM_STOP_REQUEST.value: StudyVMStopRequest,
    MonitorEvents.EVENT_STUDY_CREATED: StudyCreated,
    MonitorEvents.EVENT_STUDY_DELETED: StudyDeleted,
    MonitorEvents.EVENT_STUDY_ADD_DATASET: StudyAddDataset,
    MonitorEvents.EVENT_STUDY_REMOVE_DATASET: StudyRemoveDataset,
    MonitorEvents.EVENT_STUDY_VM_STARTED: StudyVMStarted,
    MonitorEvents.EVENT_STUDY_VM_STOPPED: StudyVMStopped,
    MonitorEvents.EVENT_USER_LOGIN.value: UserLogin,
    MonitorEvents.EVENT_USER_LOGOUT.value: UserLogout,
    MonitorEvents.EVENT_USER_IDLE.value: UserIdle,
    MonitorEvents.EVENT_USER_ACTIVE.value: UserActive,
    MonitorEvents.EVENT_DATASET_REMOVE_USER: DatasetRemoveUser,
    MonitorEvents.EVENT_DATASET_ADD_USER: DatasetAddUser,
    MonitorEvents.EVENT_DATASET_CREATED: DatasetCreated,
    MonitorEvents.EVENT_DATASET_DELETED: DatasetDeleted,
    MonitorEvents.EVENT_DATASET_ADD_DATASOURCE: DatasetAddDatasource,
    MonitorEvents.EVENT_DATASET_REMOVE_DATASOURCE: DatasetRemoveDatasource,
}


def handle_event(event_type, data):
    map_events[event_type](data).invoke()
