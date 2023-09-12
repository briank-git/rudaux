import pendulum as plm
import getpass
import prefect
from prefect import task
from prefect.engine import signals
import os
from subprocess import check_output, STDOUT, CalledProcessError
from dictauth.users import add_user, remove_user, get_users
from collections import namedtuple
import git
import shutil
from .container import run_container
from .utilities import get_logger, recursive_chown
import time

def validate_config(config):
    pass
    #config.graders
    #config.snapshot_window
    #config.grading_jupyter_user
    #config.grading_dataset_root
    #config.grading_user_quota
    #config.grading_local_collection_folder

    #config.grading_attached_student_dataset_root
    #config.grading_zfs_path
    #config.grading_jupyterhub_config_dir
    #config.instructor_repo_url
    #config.grading_docker_image
    #config.grading_docker_memory
    #config.grading_docker_bind_folder
    #config.return_solution_threshold
    #config.earliest_solution_return_date

def _clean_jhub_uname(s):
    return ''.join(ch for ch in s if ch.isalnum())

def _grader_account_name(group_name, assignment_name, user):
    return _clean_jhub_uname(group_name) + _clean_jhub_uname(assignment_name) + _clean_jhub_uname(user)

def generate_build_grading_team_name(subm_set, **kwargs):
    return 'build-grdteam-'+subm_set['__name__']

@task(checkpoint=False,task_run_name=generate_build_grading_team_name)
def build_grading_team(config, course_group, subm_set):
    logger = get_logger()
    # start by checking whether any of the assignment deadlines are in the future. If so, skip
    for course_name in subm_set:
        if course_name == '__name__':
            continue
        assignment = subm_set[course_name]['assignment']
    
        # Dict for storing course_section_id and unlock_at date key/value pairs.
        date_dict = {}
        date_dict['everyone'] = {'unlock_at' : assignment['unlock_at'], 'due_at' : assignment['due_at'], 'lock_at' : assignment['lock_at']}

        # Process per course section unlock, due, lock dates and add to date_dict
        section_overrides=[]
        for over in assignment['overrides']:
            if 'course_section_id' in over:
                section_overrides.append(over)

        #if there was at least one, get the override dates
        for over in section_overrides:
            over_dict = {}
            over_dict['unlock_at'] = over['unlock_at']
            over_dict['due_at'] = over['due_at']
            over_dict['lock_at'] = over['lock_at']
            date_dict[over['course_section_id']] = over_dict

        # Get the latest due date
        latest_due = None
        for section in date_dict.values():
            if latest_due == None or section['due_at'] > latest_due:
                latest_due = section['due_at']

        # skip the assignment if the latest due date hasn't passed yet
        if latest_due > plm.now():
            raise signals.SKIP(f"Assignment {assignment['name']} ({assignment['id']}) due date {latest_due} is in the future. Skipping.")

    # check whether all grades have been posted (assignment is done). If so, skip
    all_posted = True
    for course_name in subm_set:
        if course_name == '__name__':
            continue
        all_posted = all_posted and all([subm['posted_at'] is not None for subm in subm_set[course_name]['submissions']])
    if all_posted:
        raise signals.SKIP(f"All grades are posted for assignment {subm_set['__name__']}. Workflow done. Skipping.")

    asgn_name = subm_set['__name__']

    # get list of users from dictauth
    Args = namedtuple('Args', 'directory')
    args = Args(directory = config.jupyterhub_config_dir)
    user_tuples = get_users(args)
    dictauth_users = [u[0] for u in user_tuples]
    config_users = config.assignments[course_group][asgn_name]
    graders = []
    for user in config_users:
        # ensure user exists
        if user not in dictauth_users:
            msg = f"User account {user} listed in rudaux_config does not exist in dictauth: {dictauth_users} . Make sure to use dictauth to create a grader account for each of the TA/instructorss listed in config.assignments"
            sig = signals.FAIL(msg)
            sig.msg = msg
            raise sig
        grader = {}
        # initialize any values in the grader that are *not* potential failure points here
        grader['user'] = user
        grader['assignment_name'] = asgn_name
        grader['name'] = _grader_account_name(course_group,asgn_name,user)
        grader['unix_user'] = config.jupyterhub_user
        grader['unix_group'] = config.jupyterhub_group
        grader['unix_quota'] = config.user_quota
        grader['folder'] = os.path.join(config.user_root, grader['name']).rstrip('/')
        grader['local_source_path'] = os.path.join('source', asgn_name, asgn_name+'.ipynb')
        grader['submissions_folder'] = os.path.join(grader['folder'], config.nbgrader_path, config.submissions_folder)
        grader['autograded_folder'] = os.path.join(grader['folder'], config.nbgrader_path, config.autograded_folder)
        grader['feedback_folder'] = os.path.join(grader['folder'], config.nbgrader_path, config.feedback_folder)
        grader['workload'] = 0
        if os.path.exists(grader['submissions_folder']):
            grader['workload'] = len([f for f in os.listdir(grader['submissions_folder']) if os.path.isdir(f)])
        grader['soln_name'] = asgn_name + '_solution.html'
        grader['soln_path'] = os.path.join(grader['folder'], config.nbgrader_path, grader['soln_name'])
        graders.append(grader)

    return graders

@task
def initialize_volumes(config, graders):
    logger = get_logger()
    for grader in graders:
        # create the zfs volume
        if not os.path.exists(grader['folder']):
            logger.info(f"Grader folder {grader['folder']} doesn't exist, creating...")
            try:
                check_output(['sudo', config.zfs_path, 'create', "-o", "refquota="+grader['unix_quota'], grader['folder'].lstrip('/')], stderr=STDOUT)
            except CalledProcessError as e:
                msg = f"Error running command {e.cmd}. returncode {e.returncode}. output {e.output}. stdout {e.stdout}. stderr {e.stderr}"
                sig = signals.FAIL(msg)
                sig.msg = msg
                raise sig
            logger.info("Created!")

        # clone the git repository
        #TODO if there's an error cloning the repo or an unknown error when doing the initial test repo create
        # email instructor and print a message to tell the user to create a deploy key
        repo_valid = False
        #allow no such path or invalid repo errors; everything else should raise
        try:
            tmprepo = git.Repo(grader['folder'])
        except git.exc.InvalidGitRepositoryError as e:
            pass
        except git.exc.NoSuchPathError as e:
            pass
        else:
            repo_valid = True
        if not repo_valid:
            logger.info(f"{grader['folder']} is not a valid course repo. Cloning course repository from {config.instructor_repo_url}")
            git.Repo.clone_from(config.instructor_repo_url, grader['folder'])
            logger.info("Cloned!")
        
        # Create nbgrader config file to point it to the right course directory
        if config.nbgrader_path != "":
            f = open(os.path.join(grader['folder'], "nbgrader_config.py"), "w")
            f.writelines(["c = get_config()\n", f"c.CourseDirectory.root = \"{config.nbgrader_path}\""])
            f.close()

        # create the submissions folder
        if not os.path.exists(grader['submissions_folder']):
            os.makedirs(grader['submissions_folder'], exist_ok=True)

        aname = grader['assignment_name']

        # reassign ownership to jupyter user
        recursive_chown(grader['folder'], grader['unix_user'], grader['unix_group'])

        # if the assignment hasn't been generated yet, generate it
        # TODO error handling if the container fails

        # Construct path to nbgrader dir
        if config.nbgrader_path != "":
            nbgrader_root = os.path.join(grader['folder'], config.nbgrader_path)
        else:
            nbgrader_root = grader['folder']

        generated_asgns = run_container(config, 'nbgrader db assignment list', nbgrader_root)
        if aname not in generated_asgns['log']:
            logger.info(f"Assignment {aname} not yet generated for grader {grader['name']}")
            output = run_container(config, 'nbgrader generate_assignment --force '+aname, nbgrader_root)
            logger.info(output['log'])
            if 'ERROR' in output['log']:
                msg = f"Error generating assignment {aname} for grader {grader['name']} at path {nbgrader_root}"
                sig = signals.FAIL(msg)
                sig.msg = msg
                raise sig

        # if the solution hasn't been generated yet, generate it
        if not os.path.exists(grader['soln_path']):
            logger.info(f"Solution for {aname} not yet generated for grader {grader['name']}")
            output = run_container(config, 'jupyter nbconvert ' + grader['local_source_path'] + ' --output=' + grader['soln_name'] + ' --output-dir=.' + ' --to html', nbgrader_root)
            logger.info(output['log'])
            if 'ERROR' in output['log']:
                msg = f"Error generating solution for {aname} for grader {grader['name']} at path {nbgrader_root}"
                sig = signals.FAIL(msg)
                sig.msg = msg
                raise sig

        # transfer ownership to the jupyterhub user
        recursive_chown(grader['folder'], grader['unix_user'], grader['unix_group'])

    return graders

@task
def initialize_accounts(config, graders):
    logger = get_logger()
    for grader in graders:
        # create the jupyterhub user
        Args = namedtuple('Args', 'directory')
        args = Args(directory = config.jupyterhub_config_dir)
        output = [u[0] for u in get_users(args)]
        if grader['name'] not in output:
            logger.info(f"User {grader['name']} does not exist; creating")
            Args = namedtuple('Args', 'username directory copy_creds salt digest')
            args = Args(username = grader['name'],
                        directory = config.jupyterhub_config_dir,
                        copy_creds = grader['user'],
                        salt = None,
                        digest = None)
            add_user(args)
            check_output(['systemctl', 'stop', 'jupyterhub'])
            time.sleep(1)
            check_output(['systemctl', 'start', 'jupyterhub'])
            time.sleep(3)
    return graders
