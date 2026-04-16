from loguru import logger

from pydantic import PrivateAttr
from typing import Dict, List, Optional, Any

from canvasapi import Canvas as C

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
    _canvas_api_instances: Any = PrivateAttr(default=None)
    assignments: Dict[str, dict]

    # ---------------------------------------------------------------------------------------------------
    def model_post_init(self, context: Any) -> None:
        self._canvas_api_instances = {}
        for section, id in self.canvas_course_lms_ids.items():
            self._canvas_api_instances[section] = C(self.canvas_base_domain,self.canvas_api_tokens[section]).get_course(self.canvas_course_lms_ids[section])

    # ---------------------------------------------------------------------------------------------------
    def open(self):
        pass

    # ---------------------------------------------------------------------------------------------------
    def close(self):
        pass

    # ---------------------------------------------------------------------------------------------------
    def get_course_section_info(self, course_section_name: str) -> CourseSectionInfo:
        section = self._canvas_api_instances[course_section_name]

        processed_info = {
            "lms_id": str(section.id),
            "name": section.name,
            "code": section.course_code,
            "start_at": None if section.start_at is None else plm.parse(section.start_at),
            "end_at": None if section.end_at is None else plm.parse(section.end_at),
            "time_zone": section.time_zone
        }
        
        logger.info(f"Retrieved course section info for {section.name}")
        logger.debug(f"Processed info {processed_info}")
        if processed_info['start_at'] is None or processed_info['end_at'] is None:
            logger.warning(f"Course start or end date has not been set for section {section.name}")

        return processed_info

    # ---------------------------------------------------------------------------------------------------
    def get_students(self, course_section_name) -> Dict[str, Student]:
        section = self._canvas_api_instances[course_section_name]
        students = {}
        enrollments_dict = {e.user['id']:e for e in section.get_enrollments()}

        for s in section.get_users(enrollment_type=['student']):
            students[str(s.id)] = Student(
                                        lms_id=str(s.id), 
                                        name=s.name, 
                                        sortable_name=s.sortable_name, 
                                        school_id=s.sis_user_id, 
                                        reg_date=plm.parse(s.created_at) if s.created_at is not None else plm.parse(s.updated_at),
                                        status=enrollments_dict[s.id].enrollment_state
                                    )
            
        logger.info(f"Retrieved {len(students)} students from {section.name}")
        logger.debug(students)

        return students
    
    # ---------------------------------------------------------------------------------------------------
    def get_instructors(self, course_section_name: str) -> Dict[str, Instructor]:
        section = self._canvas_api_instances[course_section_name]
        instructors = {}
        enrollments_dict = {e.user['id']:e for e in section.get_enrollments()}

        for s in section.get_users(enrollment_type=['teacher']):
            instructors[str(s.id)] = Instructor(
                                        lms_id=str(s.id), 
                                        name=s.name, 
                                        sortable_name=s.sortable_name, 
                                        school_id=s.sis_user_id, 
                                        reg_date=plm.parse(s.created_at) if s.created_at is not None else plm.parse(s.updated_at),
                                        status=enrollments_dict[s.id].enrollment_state
                                    )
            
        logger.info(f"Retrieved {len(instructors)} instructors from {section.name}")
        logger.debug(instructors)

        return instructors

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

    