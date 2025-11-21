import fwirl
import os
import pendulum as plm
import yaml
from rudaux.flows import load_settings
from rudaux.tasks import get_grading_system
import pdb

config_path='./rudaux_config.yml'

with open(config_path) as f:
    config = yaml.safe_load(f)
#settings=load_settings(config_path)

# Test values
course_name = 'course_dsci_100_test'
course_section_name = 'section_dsci_100_test_01'
grader_name = 'test_dir_courserepo'
assignment_id = '5678'
student_id = '1234'
assignment_name = 'tutorial_intro'

gradsys = get_grading_system(settings=config, group_name=course_name)

# Initialize graph
graph = fwirl.AssetGraph("rudaux_graph")

# Should assets have reference to config?

# Submitted raw notebook in grader account directory submitted/student-*/*.ipynb
class SubmissionRawAsset(fwirl.Asset):
    def __init__(self, key, dependencies, resources = None, group = None, subgroup = None):
        self._built = False
        super(SubmissionRawAsset,self).__init__(key, dependencies, resources, group, subgroup)

    # Check if submission exists in grader's submitted directory
    async def build(self):
        grader_root = config["nbgrader_user_root"] # points to CWD in testing
        subm_folder = config["nbgrader_submissions_folder"] 
        nbgrader_path=config["nbgrader_path"]
        grader_student_id = "student-"+student_id
        subm_name = assignment_name+".ipynb"

        subm_path = os.path.join(grader_root,grader_name,nbgrader_path,subm_folder,grader_student_id,subm_name)

        if os.path.exists(subm_path):
            self._built = True
            self._ts = plm.now()
        else:
            raise Exception(f"Raw submission {subm_path} does not exist.")
        return 3

    async def timestamp(self):
        return self._ts if self._built else fwirl.AssetStatus.Unavailable

# Submitted and cleaned notebook in grader account directory submitted/student-*/*.ipynb
class SubmissionCleanedAsset(fwirl.Asset):
    def __init__(self, key, dependencies, resources = None, group = None, subgroup = None):
        self._built = False
        super(SubmissionCleanedAsset,self).__init__(key, dependencies, resources, group, subgroup)

    async def build(self):
        self._built = True
        self._ts = plm.now()
        return 3

    async def timestamp(self):
        return self._ts if self._built else fwirl.AssetStatus.Unavailable

# Autograded submission + notebook in grader account directory autograded/student-*/*.ipynb
class SubmissionAutogradedAsset(fwirl.Asset):
    def __init__(self, key, dependencies, resources = None, group = None, subgroup = None):
        self._built = False
        super(SubmissionAutogradedAsset,self).__init__(key, dependencies, resources, group, subgroup)

    async def build(self):
        self._built = True
        self._ts = plm.now()
        return 3

    async def timestamp(self):
        return self._ts if self._built else fwirl.AssetStatus.Unavailable
    
# Manually graded submission
class SubmissionManuallyGradedAsset(fwirl.Asset):
    def __init__(self, key, dependencies, resources = None, group = None, subgroup = None):
        self._built = False
        super(SubmissionManuallyGradedAsset,self).__init__(key, dependencies, resources, group, subgroup)

    async def build(self):
        self._built = True
        self._ts = plm.now()
        return 3

    async def timestamp(self):
        return self._ts if self._built else fwirl.AssetStatus.Unavailable    
    
# Feedback notebook in grader account directory feedback/student-*/*.ipynb
class GeneratedFeedbackAsset(fwirl.Asset):
    def __init__(self, key, dependencies, resources = None, group = None, subgroup = None):
        self._built = False
        super(GeneratedFeedbackAsset,self).__init__(key, dependencies, resources, group, subgroup)

    async def build(self):
        self._built = True
        self._ts = plm.now()
        return 3

    async def timestamp(self):
        return self._ts if self._built else fwirl.AssetStatus.Unavailable
    
# Initialize assets for single student and assignment

test_assets = []

submission_raw_asset = SubmissionRawAsset(
            key=f"SubmissionRaw_A{assignment_id}_S{student_id}",
            dependencies=[],
            resources=None,
            group=assignment_id,
            subgroup=student_id)

test_assets.append(submission_raw_asset)

submission_cleaned_asset = SubmissionCleanedAsset(
            key=f"SubmissionCleaned_A{assignment_id}_S{student_id}",
            dependencies=[submission_raw_asset],
            resources=None,
            group=assignment_id,
            subgroup=student_id)

test_assets.append(submission_cleaned_asset)

submission_autograded_asset = SubmissionAutogradedAsset(
            key=f"SubmissionAutoGraded_A{assignment_id}_S{student_id}",
            dependencies=[submission_cleaned_asset],
            resources=None,
            group=assignment_id,
            subgroup=student_id)

test_assets.append(submission_autograded_asset)

submission_manually_graded_asset = SubmissionManuallyGradedAsset(
            key=f"SubmissionManuallyGraded_A{assignment_id}_S{student_id}",
            dependencies=[submission_autograded_asset],
            resources=None,
            group=assignment_id,
            subgroup=student_id)

test_assets.append(submission_manually_graded_asset)

generated_feedback_asset = GeneratedFeedbackAsset(
            key=f"GeneratedFeedback_A{assignment_id}_S{student_id}",
            dependencies=[submission_manually_graded_asset],
            resources=None,
            group=assignment_id,
            subgroup=student_id)

test_assets.append(generated_feedback_asset)

graph.add_assets(test_assets)

graph.run()