from typing import Dict, Type, List
from pydantic import BaseModel

class Settings(BaseModel):
    # various constants
    prefect_queue_name : str = "rudaux-queue"
    prefect_deployment_prefix : str = "rudaux-deployment-"
    autoext_prefix : str = "autoext-"
    autoext_cron_string : str = "1,31 * * * *"
    snap_prefix : str = "snap-"
    snap_cron_string : str = "1,16,31,46 * * * *"
    grade_prefix : str = "grade-"
    grade_cron_string : str = "0 23 * * *"
    soln_prefix : str = "soln-"
    soln_cron_string : str = "30 23 * * *"
    fdbk_prefix : str = "fdbk-"
    fdbk_cron_string : str = "45 23 * * *"
    # map of course_group to list of course names in that group
    course_groups : Dict[str, List[str]]
    # maps course_group to LMS, GMS, SMS class type
    #lms_classes : Dict[str, Type]
    #gms_classes : Dict[str, Type]
    #sms_classes : Dict[str, Type]

