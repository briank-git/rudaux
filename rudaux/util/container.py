import docker
import time
# Convenience function to setup logging output. It will return a named logger, 
# which can be used inside programs. The function has the ability to setup logging 
# both to a terminal, to a log file, including setting up log rotation and for sending 
# out email on log message at warning level or above.
from .utilities import get_logger
# These Exceptions, when raised, are used to signal state changes when tasks or flows are running. 
# Signals are used in TaskRunners and FlowRunners as a way of communicating the changes in states.
from prefect.engine import signals

def run_container(config, command, homedir = None):
    client = docker.from_env() # Instantiates a client to talk to docker daemon
    logger = get_logger()
    ctr = None
    result = {}
    n_tries = 5
    # try to start the container a few times 
    while ctr is None and n_tries > 0:
        n_tries -= 1
        try:
            #start the container
            ctr = client.containers.run(config.docker_image, 
                                                command, # Command to run on the container
                                                detach = True, # Run container in the background and return a Container object.
                                                remove = False, # Remove a container when it has finished running
                                                stderr = True, # Return logs from stderr
                                                stdout = True, # Return logs from stdout
                                                mem_limit = config.docker_memory,
                                                # A dictionary to configure volumes mounted inside the container. 
                                                volumes = {homedir : {'bind': config.docker_bind_folder, 'mode': 'rw'}} if homedir else {}
                                             )
        except docker.errors.APIError as e:
            if n_tries == 0:
                raise signals.FAIL(f"Docker APIError exception encountered when starting docker container. command {command} homedir {homedir}. error message {str(e)}")
            ctr = None
            time.sleep(10.)
            if n_tries > 0:
                logger.info(f"Docker APIError exception encountered when starting docker container. command {command} homedir {homedir}. error message {str(e)}")
                logger.info(f"Failed to start container. Attempting again; {n_tries} attempts remaining.")
        except docker.errors.ImageNotFound as e:
            if n_tries == 0:
                raise signals.FAIL(f"Docker ImageNotFound exception encountered when starting docker container. command {command} homedir {homedir}. error message {str(e)}")
            ctr = None
            time.sleep(10.)
            if n_tries > 0:
                logger.info(f"Docker ImageNotFound exception encountered when starting docker container. command {command} homedir {homedir}. error message {str(e)}")
                logger.info(f"Failed to start container. Attempting again; {n_tries} attempts remaining.")
        except Exception as e:
            if n_tries == 0:
                raise signals.FAIL(f"Docker unknown exception encountered when starting docker container. command {command} homedir {homedir}. error message {str(e)}")
            ctr = None
            time.sleep(10.)
            if n_tries > 0:
                logger.info(f"Docker unknown exception encountered when starting docker container. command {command} homedir {homedir}. error message {str(e)}")
                logger.info(f"Failed to start container. Attempting again; {n_tries} attempts remaining.")
    
    # if the container started successfully, poll until it is finished
    if ctr:
        while ctr.status in ['running', 'created']:
            time.sleep(0.25)
            ctr.reload() # Update container attributes with new data
        result['exit_status'] = ctr.status
        result['log'] = ctr.logs(stdout = True, stderr = True).decode('utf-8')
        ctr.remove()
  
    # return the result
    return result


