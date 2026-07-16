from loguru import logger

import yaml

from rudaux.fwirl_components.resources import SubmissionSystemResource
from rudaux.fwirl_components.resources import LMSResource

config_path='./rudaux_config.yml'

with open(config_path) as f:
    config = yaml.safe_load(f)

ssr = SubmissionSystemResource(key='subsysresource', settings=config, course_name="course_dsci_100_test")

lmsresource = LMSResource(key='lmsresource',settings=config,min_query_interval=10,course_name="course_dsci_100_test")

assignments = lmsresource.get_assignments(course_section_name='section_dsci_100_test_01')

students = lmsresource.get_students(course_section_name='section_dsci_100_test_01')

snapshots = ssr.list_snapshots('section_dsci_100_test_01', assignments, students)

print(snapshots)