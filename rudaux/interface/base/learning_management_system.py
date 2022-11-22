from typing import List, Dict
from pydantic import BaseModel
from abc import ABC, abstractmethod
from rudaux.model import CourseSectionInfo, Instructor, Student, Assignment, Override, Submission


class LearningManagementSystem(ABC, BaseModel):
    # -----------------------------------------------------------------------------------------
    @abstractmethod
    def open(self):
        pass

    # -----------------------------------------------------------------------------------------
    @abstractmethod
    def close(self):
        pass

    # -----------------------------------------------------------------------------------------
    @abstractmethod
    def get_course_section_info(self, course_section_name: str) -> CourseSectionInfo:
        pass

    # -----------------------------------------------------------------------------------------
    @abstractmethod
    def get_students(self, course_section_name: str) -> Dict[str, Student]:
        pass

    # -----------------------------------------------------------------------------------------
    @abstractmethod
    def get_instructors(self, course_section_name: str) -> Dict[str, Instructor]:
        pass

    # -----------------------------------------------------------------------------------------
    @abstractmethod
    def get_groups(self, course_section_name: str):
        pass

    # -----------------------------------------------------------------------------------------
    @abstractmethod
    def get_assignments(self, course_group_name: str,
                        course_section_name: str) -> Dict[str, Assignment]:
        pass

    # -----------------------------------------------------------------------------------------
    @abstractmethod
    def get_submissions(self, course_group_name: str, course_section_name: str,
                        assignment: dict) -> List[Submission]:
        pass

    # -----------------------------------------------------------------------------------------
    @abstractmethod
    def update_grade(self, course_section_name: str, submission: Submission):
        pass

    # -----------------------------------------------------------------------------------------
    @abstractmethod
    def update_override(self, course_section_name: str, override: Override):
        pass

    # -----------------------------------------------------------------------------------------
    @abstractmethod
    def create_overrides(self, course_section_name: str, assignment: Assignment,
                         overrides: List[Override]):
        pass

    # -----------------------------------------------------------------------------------------
    @abstractmethod
    def delete_overrides(self, course_section_name: str, assignment: Assignment,
                         overrides: List[Override]):
        pass
    # -----------------------------------------------------------------------------------------
