from loguru import logger

import yaml

from rudaux.fwirl_components.resources import SubmissionSystemResource

config_path='./rudaux_config.yml'

with open(config_path) as f:
    config = yaml.safe_load(f)

ssr = SubmissionSystemResource(key='subsysresource', settings=config, course_name="course_dsci_100_test")

ssr.list_snapshots('section_dsci_100_test_01')