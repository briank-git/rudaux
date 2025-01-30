import os
import paramiko as pmk
from .utilities import get_logger
from prefect.engine import signals

def ssh_open(config, course_id):
    logger = get_logger()
    stu_ssh = config.student_ssh[course_id]
    logger.info(f"Opening ssh connection to {stu_ssh['hostname']}")
    # open a ssh connection to the student machine
    client = pmk.client.SSHClient()
    client.set_missing_host_key_policy(pmk.client.AutoAddPolicy())
    client.load_system_host_keys()
    # client.connect(stu_ssh['hostname'], stu_ssh['port'], stu_ssh['user'], allow_agent=True)
    client.connect(stu_ssh['hostname'], stu_ssh['port'], stu_ssh['file_user'], allow_agent=True)
    s = client.get_transport().open_session()
    pmk.agent.AgentRequestHandler(s)
    return client

# Copy file from local filesystem to remote host or remote host to local filesystem
# TAKES: Paramiko client, local file path, remote file path, fromremote=True if copying from remote
# RETURNS: True if successful, False otherwise
def copy_remote(client, localfile, remotefile, fromremote=False):
    logger = get_logger()
    sftp_client=client.open_sftp()
    try: 
        if fromremote:
            sftp_client.get(remotefile, localfile)
        else:
            sftp_client.put(localfile, remotefile)
        return True
    except Exception as e:
        logger.info(f"Failed to transfer file {localfile} {'from' if fromremote else 'to'} remote:\n {e}")
        return False
    finally:
        sftp_client.close()

# Check if file exists on remote host
# TAKES: Paramiko client, remote file path
# RETURNS: True if exists, False otherwise
def file_exists_remote(client, remotefile):
    cmd = 'test -f ' + remotefile
    # execute the snapshot command
    stdin, stdout, stderr = client.exec_command(cmd)

    exitstatus = stdout.channel.recv_exit_status()

    return not exitstatus

# Check if directory exists on remote host
# TAKES: Paramiko client, remote dir path
# RETURNS: True if exists, False otherwise
def dir_exists_remote(client, remotedir):
    cmd = 'test -d ' + remotedir
    # execute the snapshot command
    stdin, stdout, stderr = client.exec_command(cmd)

    exitstatus = stdout.channel.recv_exit_status()

    return not exitstatus