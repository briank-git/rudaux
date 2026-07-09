from rudaux.util.ssh import SSHUtil
from rudaux.interface.base.submission_system import SubmissionSystem
from rudaux.model import Assignment, Student
from rudaux.model.document import Document
from rudaux.model.snapshot import parse_snapshot_from_name, Snapshot

from loguru import logger

from typing import Dict, List, Any

class RemoteSSHSubmissions(SubmissionSystem):
    ssh_config: Dict[str, dict]

    def open(self, course_section_name: str):
        pass

    def close(self):
        pass

    # def list_snapshots(self, course_section_name: str, assignments: Dict[str, Assignment],
    #                    students: Dict[str, Student]) -> List[Snapshot]:
    #     pass

    def list_snapshots(self, course_section_name: str):
        ssh = SSHUtil()
        client = ssh.ssh_open(self.ssh_config, course_section_name)
    
    def take_snapshot(self, course_section_name: str, snapshot: Snapshot):
        pass

    
    def collect_snapshot(self, snapshot: Snapshot):
        pass

    
    def distribute(self, student: Student, document):
        pass

    