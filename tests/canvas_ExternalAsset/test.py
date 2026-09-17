import os
import pendulum as plm
import yaml
import asyncio
import pickle

import fwirl

from rudaux.flows import load_settings
from rudaux.model import Submission, Student, Grader, Assignment, CourseSectionInfo
from rudaux.interface.base.submission_system import SubmissionGradingStatus
from rudaux.fwirl_components.resources import LMSResource

import pdb

config_path='./rudaux_config.yml'

config = load_settings(config_path)

course_section_name = "section_dsci_100_test_01"

# Load LMS resource
lmsresource = LMSResource(key='lmsresource',settings=config,min_query_interval=10,course_name="course_dsci_100_test")

# Canvas course section info external asset
# Gets info about the Canvas course section (id, name, code, start at, end at, timezone)
class CanvasCourseInfoExternalAsset(fwirl.ExternalAsset):
    async def get(self):
        lmsresource = next(r for r in self.resources if r.key == 'lmsresource')
        course_section_info = lmsresource.get_course_section_info(course_section_name=course_section_name)
        return course_section_info

    # Diff start_at, end_at, and time_zone attributes
    async def diff(self, val):
        if self._cached_val is None:
            return True
        else:
            def _attrs(x):
                    return (x.start_at, x.end_at, x.time_zone)
            return _attrs(val) != _attrs(self._cached_val)

# Canvas student enrollment external asset
# Gets the class list of a Canvas course
class CanvasEnrollmentExternalAsset(fwirl.ExternalAsset):
    async def get(self):
        lmsresource = next(r for r in self.resources if r.key == 'lmsresource')
        student_enrollment = lmsresource.get_students(course_section_name=course_section_name)
        return student_enrollment

    # Diff students dict keys i.e. student lms ids, if keys are different replace cached value
    async def diff(self, val):
        if self._cached_val is None:
            return True
        else:
            return val.keys() != self._cached_val.keys()

# Canvas assignments external asset
# Gets list of Canvas assignments
class CanvasAssignmentsExternalAsset(fwirl.ExternalAsset):
    async def get(self):
        lmsresource = next(r for r in self.resources if r.key == 'lmsresource')
        assignments = lmsresource.get_assignments(course_section_name=course_section_name)
        return assignments

    # Diff assignment dict keys, if not different then compare each assignment entry
    async def diff(self, val):
        if self._cached_val is None:
            return True
        elif val.keys() != self._cached_val.keys():
            return True
        else:
            for a in val.values():
                def _attrs(x):
                    return (x.name, x.due_at, x.lock_at, x.unlock_at, x.overrides.keys(),
                            x.only_visible_to_overrides, x.published, x.skip)
                cached = self._cached_val[a.lms_id]
                if _attrs(a) != _attrs(cached):
                    return True
            return False

canvas_course_info_asset = CanvasCourseInfoExternalAsset(
            key=f"CourseInfo_C{config['canvas_course_lms_ids']['section_dsci_100_test_01']}",
            dependencies=[],
            resources=[lmsresource],
            group=None,
            subgroup=None,
            min_polling_interval=10)

canvas_enrollment_asset = CanvasEnrollmentExternalAsset(
            key=f"Enrollment_C{config['canvas_course_lms_ids']['section_dsci_100_test_01']}",
            dependencies=[],
            resources=[lmsresource],
            group=None,
            subgroup=None,
            min_polling_interval=10)

canvas_assignments_asset = CanvasAssignmentsExternalAsset(
            key=f"Assignments_C{config['canvas_course_lms_ids']['section_dsci_100_test_01']}",
            dependencies=[],
            resources=[lmsresource],
            group=None,
            subgroup=None,
            min_polling_interval=10)

# Test get and diff methods
async def main(skipget=False):
    if not skipget:
        try:
            course_info = await canvas_course_info_asset.get()
            enrollment = await canvas_enrollment_asset.get()
            assignments = await canvas_assignments_asset.get()
        except:
            print('Get method failed')

    with open("mock_course_info.pkl", "rb") as file:
        mock_course_info = pickle.load(file)
    with open("mock_enrollment.pkl", "rb") as file:
        mock_enrollment = pickle.load(file)
    with open("mock_assignments.pkl", "rb") as file:
        mock_assignments = pickle.load(file)

    # Test when _cached_val is None
    canvas_course_info_asset._cached_val = None
    canvas_enrollment_asset._cached_val = None
    canvas_assignments_asset._cached_val = None

    diff1 = await canvas_course_info_asset.diff(mock_course_info)
    diff2 = await canvas_enrollment_asset.diff(mock_enrollment)
    diff3 = await canvas_assignments_asset.diff(mock_assignments)

    if (diff1 and diff3 and diff3) is False:
        print('Failed test when _cached_val is None')
        print('Course info: ' + str(diff1))
        print('Enrollment: ' + str(diff2))
        print('Assignments: ' + str(diff3))
    else:
        print('Passed test _cached_val is None')

    # Test when synced val is same
    canvas_course_info_asset._cached_val = mock_course_info
    canvas_enrollment_asset._cached_val = mock_enrollment
    canvas_assignments_asset._cached_val = mock_assignments

    diff1 = await canvas_course_info_asset.diff(mock_course_info)
    diff2 = await canvas_enrollment_asset.diff(mock_enrollment)
    diff3 = await canvas_assignments_asset.diff(mock_assignments)

    if (diff1 or diff3 or diff3) is True:
        print('Failed test when synced val is same')
        print('Course info: ' + str(diff1))
        print('Enrollment: ' + str(diff2))
        print('Assignments: ' + str(diff3))
    else:
        print('Passed test when synced val is same')

    # Test when synced val is different
    with open("mock_course_info.pkl", "rb") as file:
        mock_course_info_diff = pickle.load(file)
    with open("mock_enrollment.pkl", "rb") as file:
        mock_enrollment_diff = pickle.load(file)
    with open("mock_assignments.pkl", "rb") as file:
        mock_assignments_diff = pickle.load(file)

    mock_course_info_diff.time_zone = 'America/New York'
    mock_enrollment_diff.popitem()
    savedname = next(iter(mock_assignments_diff.values())).name
    next(iter(mock_assignments_diff.values())).name = 'Test assignment 1234'

    diff1 = await canvas_course_info_asset.diff(mock_course_info_diff)
    diff2 = await canvas_enrollment_asset.diff(mock_enrollment_diff)
    diff3 = await canvas_assignments_asset.diff(mock_assignments_diff)

    next(iter(mock_assignments_diff.values())).name = savedname
    mock_assignments_diff.popitem()

    diff4 = await canvas_assignments_asset.diff(mock_assignments_diff)

    if (diff1 and diff2 and diff3 and diff4) is False:
        print('Failed test synced val is different')
        print('Course info: ' + str(diff1))
        print('Enrollment: ' + str(diff2))
        print('Assignments (values): ' + str(diff3))
        print('Assignments (keys): ' + str(diff4))
    else:
        print('Passed test synced val is different')

asyncio.run(main(True))