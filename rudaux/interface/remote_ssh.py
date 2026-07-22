from rudaux.util.ssh import SSHUtil
from rudaux.interface.base.submission_system import SubmissionSystem
from rudaux.model import Assignment, Student
from rudaux.model.document import Document
from rudaux.model.snapshot import parse_snapshot_from_name, Snapshot
from rudaux.util.zfs import RemoteZFS

from loguru import logger

from typing import Dict, List, Any

class RemoteSSHSubmissions(SubmissionSystem):
    ssh_config: Dict[str, dict]

    def open(self, course_section_name: str):
        pass

    def close(self):
        pass

    def list_snapshots(self, course_section_name: str, assignments: Dict[str, Assignment], students: Dict[str, Student]) -> List[Snapshot]:
        client = SSHUtil().ssh_open(self.ssh_config, course_section_name, superuser=False)
        remotezfs = RemoteZFS(client=client, tz=self.ssh_config[course_section_name]['timezone'])
        snap_dicts = remotezfs.get_snapshots(self.ssh_config[course_section_name]['student_root'])
        client.close()

        snapshots = []
        for snap_dict in snap_dicts:
            snapshot = parse_snapshot_from_name(snap_dict["name"], assignments, students)
            if snapshot is not None:
                snapshots.append(snapshot)

        return snapshots

        
    
    def take_snapshot(self, course_section_name: str, snapshot: Snapshot):
        client = SSHUtil().ssh_open(self.ssh_config, course_section_name, superuser=True)
        remotezfs = RemoteZFS(client=client, tz=self.ssh_config[course_section_name]['timezone'])
        try:
            remotezfs.take_snapshot(self.ssh_config[course_section_name]['student_root'], snapshot.get_name())
        except Exception as e:
            if "dataset already exists" in str(e).lower():
                logger.info(f"Snapshot {snapshot.get_name()} already exists.")
            else:
                raise e
        finally:
            client.close()

    
    def collect_snapshot(self, course_section_name: str, snapshot: Snapshot):
        client = SSHUtil().ssh_open(self.ssh_config, course_section_name, superuser=False)
        
        client.close()
        pass

    
    def distribute(self, course_section_name: str, student: Student, document):
        client = SSHUtil().ssh_open(self.ssh_config, course_section_name, superuser=False)
        
        client.close()
        pass

    