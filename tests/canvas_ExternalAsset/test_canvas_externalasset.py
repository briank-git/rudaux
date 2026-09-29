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

def test_canvas_diff(monkeypatch):
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
    
    with open("mock_course_info.pkl", "rb") as file:
        mock_course_info = pickle.load(file)
    with open("mock_enrollment.pkl", "rb") as file:
        mock_enrollment = pickle.load(file)
    with open("mock_assignments.pkl", "rb") as file:
        mock_assignments = pickle.load(file)

    # Test when _cached_val is None
    course_info_asset._cached_val = None
    students_list_asset._cached_val = None
    assignments_list_asset._cached_val = None

    diff1_1 = course_info_asset.diff(mock_course_info)
    diff1_2 = students_list_asset.diff(mock_enrollment)
    diff1_3 = assignments_list_asset.diff(mock_assignments)

    assert diff1_1 is True
    assert diff1_2 is True
    assert diff1_3 is True

    # Test when synced val is same
    course_info_asset._cached_val = mock_course_info
    students_list_asset._cached_val = mock_enrollment
    assignments_list_asset._cached_val = mock_assignments

    diff2_1 = course_info_asset.diff(mock_course_info)
    diff2_2 = students_list_asset.diff(mock_enrollment)
    diff2_3 = assignments_list_asset.diff(mock_assignments)

    assert diff2_1 is False
    assert diff2_2 is False
    assert diff2_3 is False

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

    diff3_1 = course_info_asset.diff(mock_course_info_diff)
    diff3_2 = students_list_asset.diff(mock_enrollment_diff)
    diff3_3 = assignments_list_asset.diff(mock_assignments_diff)

    next(iter(mock_assignments_diff.values())).name = savedname
    mock_assignments_diff.popitem()

    diff3_4 = assignments_list_asset.diff(mock_assignments_diff)

    assert diff3_1 is True
    assert diff3_2 is True
    assert diff3_3 is True
    assert diff3_4 is True