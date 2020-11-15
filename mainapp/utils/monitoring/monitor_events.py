from enum import Enum


class MonitorEvents(Enum):
    EVENT_USER_LOGIN = "user_login"
    EVENT_USER_LOGOUT = "user_logout"
    EVENT_USER_ACTIVE = "user_active"
    EVENT_USER_IDLE = "user_idle"
    EVENT_USER_SIGNED_EULA = "user_signed_eula"

    EVENT_REQUEST_NOTEBOOK = "request_notebook"
    EVENT_NOTEBOOK_READY = "notebook_ready"
    EVENT_NOTEBOOK_LOAD_FAIL = "notebook_load_fail"

    EVENT_STUDY_VM_START_REQUEST = "study_vm_start_request"
    EVENT_STUDY_VM_STOP_REQUEST = "study_vm_stop_request"
    EVENT_STUDY_VM_STARTED = "study_vm_started"
    EVENT_STUDY_VM_STOPPED = "study_vm_stopped"

    EVENT_DATASET_CREATED = "dataset_created"
    EVENT_DATASET_ADD_USER = "dataset_add_user"
    EVENT_DATASET_REMOVE_USER = "dataset_remove_user"
    EVENT_DATASET_DELETED = "dataset_deleted"
    EVENT_DATASET_ADD_DATASOURCE = "dataset_add_datasource"
    EVENT_DATASET_REMOVE_DATASOURCE = "dataset_remove_datasource"

    EVENT_STUDY_CREATED = "study_created"
    EVENT_STUDY_DELETED = "study_deleted"
    EVENT_STUDY_ADD_DATASET = "study_add_dataset"
    EVENT_STUDY_REMOVE_DATASET = "study_remove_dataset"
