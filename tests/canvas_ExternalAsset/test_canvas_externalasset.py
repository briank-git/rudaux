import os
import pendulum as plm
import yaml
import asyncio
import pickle
import pytest

import fwirl

from rudaux.flows import load_settings
from rudaux.model import Submission, Student, Grader, Assignment, CourseSectionInfo
from rudaux.interface.base.submission_system import SubmissionGradingStatus
from rudaux.fwirl_components.resources import LMSResource
from rudaux.fwirl_components.assets import CourseInfoAsset, AssignmentsListAsset, StudentsListAsset

import pdb

# Test get and diff methods

@pytest.mark.asyncio
async def test_canvas_get(monkeypatch):
    monkeypatch.chdir("tests/canvas_ExternalAsset")

    config_path='./rudaux_config.yml'

    config = load_settings(config_path)

    course_section_name = "section_dsci_100_test_01"

    # Load LMS resource
    lms_resource = LMSResource(key='lmsresource',settings=config,min_query_interval=10,course_name="course_dsci_100_test")

    course_info_asset = CourseInfoAsset(
                key=f"CourseInfo_C{config['canvas_course_lms_ids']['section_dsci_100_test_01']}",
                dependencies=[],
                lms_resource = lms_resource,
                course_section_name = course_section_name,
                min_polling_interval=10)

    students_list_asset = StudentsListAsset(
                key=f"StudentsList_C{config['canvas_course_lms_ids']['section_dsci_100_test_01']}",
                dependencies=[],
                lms_resource = lms_resource,
                course_section_name = course_section_name,
                min_polling_interval=10)

    assignments_list_asset = AssignmentsListAsset(
                key=f"AssignmentsList_C{config['canvas_course_lms_ids']['section_dsci_100_test_01']}",
                dependencies=[],
                lms_resource = lms_resource,
                course_section_name = course_section_name,
                min_polling_interval=10)

    try:
        course_info = await course_info_asset.get()
        students = await students_list_asset.get()
        assignments = await assignments_list_asset.get()
    except Exception as e:
        pytest.fail('Get method failed. Reason: ' + str(e))
    assert (course_info and students and assignments) is not None




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