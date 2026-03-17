from loguru import logger

from typing import Dict, List

from canvasapi import Canvas

import pendulum as plm
from pendulum import DateTime
from pendulum.tz.timezone import Timezone

from rudaux.interface.base.learning_management_system import LearningManagementSystem
from rudaux.interface.base.submission_system import SubmissionGradingStatus
from rudaux.model.course_section_info import CourseSectionInfo
from rudaux.model.assignment import Assignment
from rudaux.model.student import Student
from rudaux.model.instructor import Instructor
from rudaux.model.submission import Submission
from rudaux.model.override import Override

class Canvas(LearningManagementSystem):
    canvas_base_domain: str
    canvas_course_lms_ids: Dict[str, str]
    canvas_registration_deadlines: Dict[str, str]
    canvas_api_tokens: Dict[str, str]
    canvas_api_instances: Dict[str, Canvas]
    assignments: Dict[str, dict]

    # ---------------------------------------------------------------------------------------------------
    def open(self):
        pass

    # ---------------------------------------------------------------------------------------------------
    def close(self):
        pass

    # ---------------------------------------------------------------------------------------------------
    def get_course_section_info(self, course_section_name: str) -> CourseSectionInfo:
        pass

    # ---------------------------------------------------------------------------------------------------
    def get_students(self, course_section_name) -> Dict[str, Student]:
        pass

    # ---------------------------------------------------------------------------------------------------
    def get_instructors(self, course_section_name: str) -> Dict[str, Instructor]:
        pass

    # ---------------------------------------------------------------------------------------------------
    def get_tas(self, course_section_name: str):
        pass

    # ---------------------------------------------------------------------------------------------------
    def get_groups(self, course_section_name: str):
        pass

    # ---------------------------------------------------------------------------------------------------
    def get_assignments(self, course_group_name: str, course_section_name: str) -> Dict[str, Assignment]:
        pass

    # ---------------------------------------------------------------------------------------------------
    def get_submissions(self, course_group_name: str, course_section_name: str, assignment: Assignment) -> List[Submission]:
        pass
        
    # ---------------------------------------------------------------------------------------------------
    def update_grade(self, course_section_name: str, submission: Submission):
        pass

    # ---------------------------------------------------------------------------------------------------
    def update_override(self, course_name: str, override: Override):
        pass

    # ---------------------------------------------------------------------------------------------------
    def create_overrides(self, course_section_name: str, assignment: Assignment, overrides: List[Override]):
        pass

    # ---------------------------------------------------------------------------------------------------
    def delete_overrides(self, course_section_name: str, assignment: Assignment, overrides: List[Override]):
        pass

    