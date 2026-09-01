import os
import pendulum as plm
import yaml

import fwirl

from rudaux.flows import load_settings
#from rudaux.tasks import get_grading_system
from rudaux.model import Submission, Student, Grader, Assignment, CourseSectionInfo
from rudaux.fwirl_components.resources import GradingSystemResource
from rudaux.interface.base.submission_system import SubmissionGradingStatus

import pdb

config_path='./rudaux_config.yml'

with open(config_path) as f:
    config = yaml.safe_load(f)


# Canvas course section info external asset
# Gets info about the Canvas course section (id, name, code, start at, end at, timezone)
class CanvasCourseInfoExternalAsset(fwirl.ExternalAsset):
    pass

# Canvas enrollment external asset
# Gets the class list of a Canvas course
class CanvasEnrollmentExternalAsset(fwirl.ExternalAsset):
    pass

# Canvas assignments external asset
# Gets list of Canvas assignments
class CanvasAssignmentsExternalAsset(fwirl.ExternalAsset):
    pass

