import os
import pendulum as plm
import yaml

import fwirl

from rudaux.flows import load_settings
from rudaux.model import Submission, Student, Grader, Assignment, CourseSectionInfo
from rudaux.interface.base.submission_system import SubmissionGradingStatus
from rudaux.fwirl_components.resources import LMSResource

import pdb

config_path='./rudaux_config.yml'

config = load_settings(config_path)

# Load LMS resource
lmsresource = LMSResource(key='lmsresource',settings=config,min_query_interval=10,course_name="course_dsci_100_test")

# Canvas course section info external asset
# Gets info about the Canvas course section (id, name, code, start at, end at, timezone)
class CanvasCourseInfoExternalAsset(fwirl.ExternalAsset):
    async def get(self):
        course_section_info = lmsresource.get_course_section_info(course_section_name='section_dsci_100_test_01')
        return course_section_info

    async def diff(self, val):
        if self._cached_val is None:
            return True
        else:
            return val != self._cached_val

# Canvas student enrollment external asset
# Gets the class list of a Canvas course
class CanvasEnrollmentExternalAsset(fwirl.ExternalAsset):
    async def get(self):
        student_enrollment = lmsresource.get_students(course_section_name='section_dsci_100_test_01')
        return student_enrollment

    async def diff(self, val):
        if self._cached_val is None:
            return True
        else:
            return val != self._cached_val

# Canvas assignments external asset
# Gets list of Canvas assignments
class CanvasAssignmentsExternalAsset(fwirl.ExternalAsset):
    async def get(self):
        assignments = lmsresource.get_assignments(course_section_name='section_dsci_100_test_01')
        return assignments

    async def diff(self, val):
        if self._cached_val is None:
            return True
        else:
            return val != self._cached_val

