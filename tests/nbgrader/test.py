from loguru import logger

import yaml

from rudaux.fwirl_components.resources import GradingSystemResource
from rudaux.model import Grader

config_path='./rudaux_config.yml'

with open(config_path) as f:
    config = yaml.safe_load(f)

gradingsystemresource = GradingSystemResource(settings=config, course_name="course_dsci_100_test")

