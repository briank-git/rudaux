from loguru import logger

import yaml

from rudaux.fwirl_components.resources import GradingSystemResource
from rudaux.model import Grader

config_path='./rudaux_config.yml'

with open(config_path) as f:
    config = yaml.safe_load(f)

gsr = GradingSystemResource(key='gradsysresource',settings=config, course_name="course_dsci_100_test")

# Testing vars
course_name="course_dsci_100_test"
assignment_name="tutorial_intro"
username="mockgrader"
skip=False

# Build grader object

grader1 = gsr.build_grader(course_name, assignment_name, username, skip)
print(grader1)

# Initialize grader
# create_grading_volume
# _clone_git_repository(grader=grader)
# _create_submission_folder(grader=grader)
# generate_assignment(grader=grader)
# generate_solution(grader=grader)
# _initialize_account(grader=grader)
gsr.initialize_graders([grader1])


