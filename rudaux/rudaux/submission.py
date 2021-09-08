from traitlets.config.configurable import Configurable
from traitlets import Int, Float, Unicode, Bool
from enum import IntEnum
import os, shutil, pwd
import json
from nbgrader.api import Gradebook, MissingEntry
from .docker import DockerError
#from .course_api import GradeNotUploadedError
import pendulum as plm
from .course_api import put_grade
from .container import run_container
import prefect
from prefect import task

class GradingStatus(IntEnum):
    ASSIGNED = 0
    NOT_DUE = 1
    MISSING = 2
    COLLECTED = 3
    PREPARED = 4
    AUTOGRADED = 6
    NEEDS_MANUAL_GRADE = 9
    DONE_GRADING = 10

def validate_config(config):
    # config.student_dataset_root
    # config.student_local_assignment_folder
    return config


# keep this function separate despite how simple it is
# in the future we may want to create 1 task per submission (right now it's 1 task per assignment to
# avoid network IO blowup with current Prefect 0.15.5)
# if eventually it is possible to do 1 task per submission, then this is where
# you would output a flattened list of every (student, assignment) pair
@task(checkpoint=False)
def initialize_submission_sets(config, course_infos, assignments, students):
    logger = prefect.context.get("logger")
    # verify that each list is of length (# courses)
    if len(course_infos) != len(assignments) or len(course_infos) != len(students):
        sig = signals.FAIL(f"course_infos, assignments, and students lists must all have the same number "+
                           f"of elements (number of courses in the group). ids: {len(course_infos)} assignments: {len(assignments)} students: {len(students)}")
        sig.course_infos = course_infos
        sig.assignments = assignments
        sig.students = students
        raise sig

    # if the lists are empty, just return empty submissions
    if len(course_infos) == 0:
        subms = []
        return subms

    # build the map from assignment name to indices
    asgn_map = {}
    for i in range(len(assignments)):
        a_list = assignments[i]
        for j in range(len(a_list)):
            if a_list[j]['name'] not in asgn_map:
                asgn_map[a_list[j]['name']] = len(assignments)*[None]
            asgn_map[a_list[j]['name']][i] = j

    # if None is still present in any of the lists, then there is
    # an assignment in one course not present in another; sig.FAIL
    if any([None in v for k, v in asgn_map.items()]):
        sig = signals.FAIL(f"one course has an assignment not present in another. Assignment index mapping: {asgn_map}")
        sig.course_infos = course_infos
        sig.assignments = assignments
        sig.students = students
        raise sig

    # construct the list of grouped assignments
    # data structure:
    # list of dicts, one for each assignment
    #     '__name__' : (assignment group name)
    #     'course_name' : {
    #              'assignment' : (assignment object)
    #              'submissions' : [  {
    #                                  'student' : (student object)
    #                                  'name'    : (submission name)
    subm_sets = []
    for name in asgn_map:
        subm_set = {}
        subm_set['__name__'] = name
        for i in range(len(course_infos)):
            course_name = config.course_names[course_infos[i]['id']]
            course_info = course_infos[i]
            assignment = assignments[i][asgn_map[name][i]]
            subm_set[course_name] = {}
            subm_set[course_name]['course_info'] = course_info
            subm_set[course_name]['assignment'] = assignment
            subm_set[course_name]['submissions'] = [{
                                        'student' : stu,
                                        'name' : f"{course_name}-{course_info['id']} : {assignment['name']}-{assignment['id']} : {stu['name']}-{stu['id']}"
                                        } for stu in students[i] if stu['status'] == 'active']
        subm_sets.append(subm_set)

            
    logger.info(f"Built a list of {len(subm_sets)} submission sets")
    return subm_sets

def _get_due_date(assignment, student):
    basic_date = assignment['due_at']

    #get overrides for the student
    overrides = [over for over in assignment['overrides'] if student['id'] in over['student_ids'] and (over['due_at'] is not None)]

    #if there was no override, return the basic date
    if len(overrides) == 0:
        return basic_date, None

    #if there was one, get the latest override date
    latest_override = overrides[0]
    for over in overrides:
        if over['due_at'] > latest_override['due_at']:
            latest_override = over

    #return the latest date between the basic and override dates
    if latest_override['due_at'] > basic_date:
        return latest_override['due_at'], latest_override
    else:
        return basic_date, None

def generate_compute_deadlines_name(subm_set, **kwargs):
    return 'get-deadlns-'+subm_set['__name__']

@task(checkpoint=False,task_run_name=generate_compute_deadlines_name)
def compute_deadlines(subm_set):
    for course_name in subm_set:
        if course_name == '__name__':
            continue
        assignment = subm_set[course_name]['assignment']
        course_info = subm_set[course_name]['course_info']
        for subm in subm_set[course_name]['submissions']:
            student = subm['student']

            # check student regdate, assignment due/unlock dates exist
            if assignment['unlock_at'] is None or assignment['due_at'] is None:
                sig = signals.FAIL(f"Invalid unlock ({assignment['unlock_at']}) and/or due ({assignment['due_at']}) date for assignment {assignment['name']}")
                sig.assignment = assignment
                raise sig

            # if assignment dates are prior to course start, error
            if assignment['unlock_at'] < course_info['start_at'] or assignment['due_at'] < course_info['start_at']:
                sig = signals.FAIL(f"Assignment {assignment['name']} unlock date ({assignment['unlock_at']}) "+
                                   f"and/or due date ({assignment['due_at']}) is prior to the course start date "+
                                   f"({course_info['start_at']}). This is often because of an old deadline from "+
                                   f"a copied Canvas course from a previous semester. Please make sure assignment "+
                                   f"deadlines are all updated to the current semester.")
                sig.assignment = assignment
                raise sig

            # if student has no reg date, error
            if student['reg_date'] is None:
                sig = signals.FAIL(f"Invalid registration date for student {student['name']}, {student['id']} ({student['reg_date']})")
                sig.student = student
                raise sig

            # compute the assignment's due date
            due_date, override = _get_due_date(assignment, student)
            subm['due_at'] = due_date
            subm['override'] = override
    return subm_set

def generate_latereg_overrides_name(extension_days, subm_set, **kwargs):
    return 'lateregs-'+subm_set['__name__']

@task(checkpoint=False,task_run_name=generate_latereg_overrides_name)
def get_latereg_overrides(extension_days, subm_set):
    logger = prefect.context.get("logger")
    fmt = 'ddd YYYY-MM-DD HH:mm:ss'
    overrides = []
    for course_name in subm_set:
        if course_name == '__name__':
            continue
        assignment = subm_set[course_name]['assignment']
        course_info = subm_set[course_name]['course_info']
        tz = course_info['time_zone']
        for subm in subm_set[course_name]['submissions']:
            student = subm['student']
            regdate = student['reg_date']
            override = subm['override']

            to_remove = None
            to_create = None
            if regdate > assignment['unlock_at']:
                logger.info(f"Student {student['name']} needs an extension on assignment {assignment['name']}")
                logger.info(f"Student registration date: {regdate}    Status: {student['status']}")
                logger.info(f"Assignment unlock: {assignment['unlock_at']}    Assignment deadline: {assignment['due_at']}")
                #the late registration due date
                latereg_date = regdate.add(days=extension_days).in_timezone(tz).end_of('day')
                logger.info("Current student-specific due date: " + subm['due_at'].in_timezone(tz).format(fmt) + " from override: " + str(True if (override is not None) else False))
                logger.info('Late registration extension date: ' + latereg_date.in_timezone(tz).format(fmt))
                if latereg_date > subm['due_at']:
                    logger.info('Creating automatic late registration extension to ' + latereg_date.in_timezone(tz).format(fmt))
                    if override is not None:
                        logger.info("Need to remove old override " + str(override['id']))
                        to_remove = override
                    to_create = {'student_ids' : [student['id']],
                                 'due_at' : latereg_date,
                                 'lock_at' : assignment['lock_at'],
                                 'unlock_at' : assignment['unlock_at'],
                                 'title' : student['name']+'-'+assignment['name']+'-latereg'}
                else:
                    continue
                    #raise signals.SKIP("Current due date for student {student['name']}, assignment {assignment['name']} after late registration date; no override modifications required.")
            else:
                continue
                #raise signals.SKIP("Assignment {assignment['name']} unlocks after student {student['name']} registration date; no extension required.")
            overrides.append((assignment, to_create, to_remove))
            #return (assignment, to_create, to_remove)
    return overrides

@task(checkpoint=False)
def assign_graders(submissions, graders):
    # search for this student in the grader folders
    found = False
    for grader in graders:
        collected_assignment_path = os.path.join(grader['submissions_folder'],
                                                 config.grading_student_folder_prefix+stu['id'],
                                                 asgn['name'] + '.ipynb')
        if os.path.exists(collected_assignment_path):
            found = True
            subm['grader'] = grader
            break

    # if not assigned to anyone, choose the worker with the minimum current workload
    if not found:
        # TODO I believe sorted makes a copy, so I need to find the original grader in the list to increase their workload
        # should debug this to make sure workloads are actually increasing, and also figure out whether it's possible to simplify
        min_grader = sorted(graders, key = lambda g : g['workload'])[0]
        graders[graders.index(min_grader)]['workload'] += 1
        subm['grader'] = graders[graders.index(min_grader)]

    subm['name'] = f"({asgn['name']}-{asgn['id']},{stu['name']}-{stu['id']},{subm['grader']['name']})"

    subms.append(subm)
    return subms

# validate each submission, skip if not due yet
@task(checkpoint=False)
def build_subms(config, course_info, subm):
    logger = prefect.context.get("logger")
    logger.info(f"Validating submission {submission['name']}")
    assignment = subm['assignment']
    student = subm['student']

    # check student regdate, assignment due/unlock dates exist
    if assignment['unlock_at'] is None or assignment['due_at'] is None:
        sig = signals.FAIL(f"Invalid unlock ({assignment['unlock_at']}) and/or due ({assignment['due_at']}) date for assignment {assignment['name']}")
        sig.assignment = assignment
        raise sig
    if assignment['unlock_at'] < course_info['start_at'] or assignment['due_at'] < course_info['start_at']:
        sig = signals.FAIL(f"Assignment {assignment['name']} unlock date ({assignment['unlock_at']}) "+
                            f"and/or due date ({assignment['due_at']}) is prior to the course start "+
                            f"date ({course_info['start_at']}). This is often because of an old deadline "+
                            f"from a copied Canvas course from a previous semester. Please make sure assignment "+
                            f"deadlines are all updated to the current semester.")
        sig.assignment = assignment
        raise sig
    if student['reg_date'] is None:
        sig = signals.FAIL(f"Invalid registration date for student {student['name']}, {student['id']} ({student['reg_date']})")
        sig.student = student
        raise sig

    # if student is inactive, skip
    if student['status'] != 'active':
        raise signals.SKIP(f"Student {student['name']} is inactive. Skipping their submissions.")

    # initialize values that are potential failure points here
    due_date, override = _get_due_date(assignment, student)
    subm['due_at'] = due_date
    subm['override'] = override
    subm['snap_name'] = _get_snap_name(config, assignment, override)
    if override is None:
        subm['zfs_snap_path'] = config.student_dataset_root.strip('/') + '@' + subm['snap_name']
    else:
        subm['zfs_snap_path'] = os.path.join(config.student_dataset_root, student['id']).strip('/') + '@' + subm['snap_name']
    subm['attached_folder'] = os.path.join(config.grading_attached_student_dataset_root, student['id'])
    subm['snapped_assignment_path'] = os.path.join(subm['attached_folder'],
                '.zfs', 'snapshot', subm['snap_name'], config.student_local_assignment_folder,
                assignment['name'], assignment['name']+'.ipynb')
    subm['soln_path'] = os.path.join(subm['attached_folder'], assignment['name'] + '_solution.html')
    subm['fdbk_path'] = os.path.join(subm['attached_folder'], assignment['name'] + '_feedback.html')
    subm['score'] = subm_info[assignment['id']][student['id']]['score']
    subm['posted_at'] = subm_info[assignment['id']][student['id']]['posted_at']
    subm['late'] = subm_info[assignment['id']][student['id']]['late']
    subm['missing'] = subm_info[assignment['id']][student['id']]['missing']
    subm['collected_assignment_path'] = os.path.join(subm['grader']['submissions_folder'],
                                                     config.grading_student_folder_prefix+subm['student']['id'],
                                                     subm['assignment']['name'], subm['assignment']['name'] + '.ipynb')
    subm['autograded_assignment_path'] = os.path.join(subm['grader']['autograded_folder'],
                                                     config.grading_student_folder_prefix+subm['student']['id'],
                                                     subm['assignment']['name'], subm['assignment']['name'] + '.ipynb')
    subm['feedback_path'] = os.path.join(subm['grader']['feedback_folder'],
                                                     config.grading_student_folder_prefix+subm['student']['id'],
                                                     subm['assignment']['name'], subm['assignment']['name'] + '.html')
    subm['status'] = GradingStatus.ASSIGNED

    return subm

@task(checkpoint=False)
def get_pastdue_fractions(config, course_info, submissions):
    assignment_totals = {}
    assignment_outstanding = {}
    assignment_fracs = {}
    for subm in submissions:
        assignment = subm['assignment']
        anm = assignment['name']
        if anm not in assignment_totals:
            assignment_totals[anm] = 0
        if anm not in assignment_outstanding:
            assignment_outstanding[anm] = 0

        assignment_totals[anm] += 1
        if subm['due_at'] > plm.now():
            assignment_outstanding[anm] += 1

    for k, v in assignment_totals.items():
        assignment_fracs[k] = (v - assignment_outstanding[k])/v

    return assignment_fracs

@task(checkpoint=False)
def return_solution(config, course_info, assignment_fracs, subm):
    logger = prefect.context.get("logger")
    assignment = subm['assignment']
    anm = assignment['name']
    logger.info(f"Checking whether solution for submission {subm['name']} can be returned")
    if subm['due_at'] > plm.now() and assignment_fracs[anm] > config.return_solution_threshold and plm.now() > plm.parse(config.earliest_solution_return_date, tz=course_info['time_zone']):
        logger.info(f"Returning solution submission {subm['name']}")
        if not os.path.exists(subm['soln_path']):
            if os.path.exists(subm['attached_folder']):
                try:
                    shutil.copy(subm['grader']['soln_path'], subm['soln_path'])
                    os.chown(subm['soln_path'], subm['grader']['unix_uid'], subm['grader']['unix_uid'])
                except Exception as e:
                    raise signals.FAIL(str(e))
            else:
                logger.warning(f"Warning: student folder {subm['attached_folder']} doesnt exist. Skipping solution return.")
    else:
        logger.info(f"Not returnable yet. Either the student-specific due date ({subm['due_at']}) has not passed, threshold not yet reached ({assignment_fracs[anm]} <= {config.return_solution_threshold}) or not yet reached the earliest possible solution return date")

    return submission

@task(checkpoint=False)
def collect_submission(config, subm):
    logger = prefect.context.get("logger")
    logger.info(f"Collecting submission {subm['name']}...")
    # if the submission is due in the future, skip
    if subm['due_at'] > plm.now():
         subm['status'] = GradingStatus.NOT_DUE
         raise signals.SKIP(f"Submission {subm['name']} is due in the future. Skipping.")

    if not os.path.exists(subm['collected_assignment_path']):
        if not os.path.exists(subm['snapped_assignment_path']):
            logger.info(f"Submission {subm['name']} is missing. Uploading score of 0.")
            subm['status'] = GradingStatus.MISSING
            subm['score'] = 0.
            put_grade(config, subm)
            raise signals.SKIP(f"Skipping the remainder of the task flow for submission {subm['name']}.")
        else:
            shutil.copy(subm['snapped_assignment_path'], subm['collected_assignment_path'])
            os.chown(subm['collected_assignment_path'], subm['grader']['unix_uid'], subm['grader']['unix_uid'])
            subm['status'] = GradingStatus.COLLECTED
            logger.info("Submission collected.")
    else:
        logger.info("Submission already collected.")
        subm['status'] = GradingStatus.COLLECTED
    return subm

@task(checkpoint=False)
def clean_submission(config, subm):
    logger = prefect.context.get("logger")

    #need to check for duplicate cell ids, see
    #https://github.com/jupyter/nbgrader/issues/1083
    #open the student's notebook
    f = open(subm['collected_assignment_path'], 'r')
    nb = json.load(f)
    f.close()

    #go through and delete the nbgrader metadata from any duplicated cells
    cell_ids = set()
    for cell in nb['cells']:
      try:
        cell_id = cell['metadata']['nbgrader']['grade_id']
      except:
        continue
      if cell_id in cell_ids:
        logger.info(f"Student {student['name']} assignment {assignment['name']} grader {grader} had a duplicate cell! ID = {cell_id}")
        logger.info("Removing the nbgrader metainfo from that cell to avoid bugs in autograde")
        cell['metadata'].pop('nbgrader', None)
      else:
        cell_ids.add(cell_id)

    #write the sanitized notebook back to the submitted folder
    f = open(subm['collected_assignment_path'], 'w')
    json.dump(nb, f)
    f.close()

    subm['status'] = GradingStatus.PREPARED

    return subm

@task(checkpoint=False)
def autograde(config, subm):
    logger = prefect.context.get("logger")
    logger.info(f"Autograding submission {subm['name']}")

    if os.path.exists(subm['autograded_assignment_path']):
        logger.info('Assignment previously autograded & validated.')
        subm['status'] = GradingStatus.AUTOGRADED
        return subm

    logger.info('Removing old autograding result from DB if it exists')
    try:
        gb = Gradebook('sqlite:///'+os.path.join(subm['grader']['folder'], 'gradebook.db'))
        gb.remove_submission(subm['assignment']['name'], config.grading_student_folder_prefix+subm['student']['id'])
    except MissingEntry as e:
        pass
    finally:
        gb.close()
    logger.info('Autograding...')
    res = run_container(config, 'nbgrader autograde --force --assignment=' + subm['assignment']['name'] + ' --student='+config.grading_student_folder_prefix+subm['student']['id'], subm['grader']['folder'])

    # validate the results
    if 'ERROR' in res['log']:
        raise signals.FAIL(f"Docker error autograding submission {subm['name']}: exited with status {res['exit_status']},  {res['log']}")
    if not os.path.exists(subm['autograded_assignment_path']):
        raise signals.FAIL(f"Docker error autograding submission {subm['name']}: did not generate expected file at {subm['autograded_assignment_path']}")

    return subm

@task(checkpoint=False)
def wait_for_manual_grading(config, subm):
    logger = prefect.context.get("logger")
    logger.info(f"Checking whether submission {subm['name']} needs manual grading")

    # check if the submission needs manual grading
    try:
        gb = Gradebook('sqlite:///'+os.path.join(subm['grader']['folder'], 'gradebook.db'))
        gb_subm = gb.find_submission(subm['assignment']['name'], config.grading_student_folder_prefix+subm['student']['id'])
        flag = gb_subm.needs_manual_grade
    except Exception as e:
        sig = signals.FAIL(f"Error when checking whether submission {subm['name']} needs manual grading; error {str(e)}")
        sig.e = e
        sig.subm = subm
        raise sig
    finally:
        gb.close()

    if flag:
        subm['status'] = GradingStatus.NEEDS_MANUAL_GRADE
        raise signals.SKIP(f"Submission {subm['name']} still waiting for manual grading. Skipping the remainder of this task.")

    logger.info("Done grading for submission {subm['name']}.")
    subm['status'] = GradingStatus.DONE_GRADING
    return subm

@task(checkpoint=False,skip_on_upstream_skip = False)
def get_complete_assignments(config, assignments, submissions):
    complete_tokens = []
    for asgn in assignments:
        if all([ (subm['status'] == GradingStatus.DONE_GRADING or subm['status'] == GradingStatus.MISSING) for subm in submissions if subm['assignment']['id'] == asgn['id']]):
            complete_tokens.append(asgn['id'])
    return complete_tokens

@task(checkpoint=False)
def wait_for_completion(config, complete_ids, subm):
    if subm['assignment']['id'] in complete_ids:
        raise signals.SKIP("Submission {subm['name']} : other submissions for this assignment not done grading yet. Skipping remainder of this workflow (uploading grades / returning feedback)")
    return subm

@task(checkpoint=False)
def generate_feedback(config, subm):
    logger = prefect.context.get("logger")
    logger.info(f"Generating feedback for submission {subm['name']}")

    if os.path.exists(subm['feedback_path']):
        logger.info('Feedback generated previously.')
        subm['status'] = GradingStatus.FEEDBACK_GENERATED
        return subm
    res = run_container(config, 'nbgrader generate_feedback --force --assignment=' + subm['assignment']['name'] + ' --student=' + config.grading_student_folder_prefix+subm['student']['id'], subm['grader']['folder'])

    # validate the results
    if 'ERROR' in res['log']:
        raise signals.FAIL(f"Docker error generating feedback for submission {subm['name']}: exited with status {res['exit_status']},  {res['log']}")
    if not os.path.exists(subm['feedback_path']):
        raise signals.FAIL(f"Docker error generating feedback for submission {subm['name']}: did not generate expected file at {subm['feedback_path']}")

    subm['status'] = GradingStatus.FEEDBACK_GENERATED
    return subm


# TODO this func still needs some work
@task(checkpoint=False)
def return_feedback(config, course_info, assignment_fracs, subm):
    logger = prefect.context.get("logger")
    assignment = subm['assignment']
    anm = assignment['name']
    logger.info(f"Checking whether feedback for submission {subm['name']} can be returned")
    if subm['due_at'] > plm.now() and assignment_fracs[anm] > config.return_solution_threshold and plm.now() > plm.parse(config.earliest_solution_return_date, tz=course_info['time_zone']):
        logger.info(f"Returning feedback for submission {subm['name']}")
        if not os.path.exists(fdbk_path_student):
            if os.path.exists(fdbk_folder_student):
                try:
                    shutil.copy(fdbk_path_grader, fdbk_path_student)
                    os.chown(fdbk_path_student, subm['grader']['unix_uid'], subm['grader']['unix_uid'])
                except Exception as e:
                    print('Error occured when returning feedback.')
                    print(e)
                    self.error = e
                    return SubmissionStatus.ERROR
            else:
                print('Warning: student folder ' + str(fdbk_folder_student) + ' doesnt exist. Skipping feedback return.')
    else:
        logger.info(f"Feedback not returnable yet. Either the threshold has not yet been reached ({assignment_fracs[anm]} <= {config.return_solution_threshold}) or not yet reached the earliest possible solution return date")
    return subm


def _compute_max_score(config, subm):
  #for some incredibly annoying reason, nbgrader refuses to compute a max_score for anything (so we cannot easily convert scores to percentages)
  #let's compute the max_score from the notebook manually then....
  release_nb_path = os.path.join(subm['grader']['folder'], 'release', subm['assignment']['name'], subm['assignment']['name']+'.ipynb')
  f = open(release_nb_path, 'r')
  parsed_json = json.load(f)
  f.close()
  pts = 0
  for cell in parsed_json['cells']:
    try:
      pts += cell['metadata']['nbgrader']['points']
    except Exception as e:
      #will throw exception if cells dont exist / not right type -- that's fine, it'll happen a lot.
      pass
  return pts

@task(checkpoint=False)
def upload_grade(config, subm):
    logger = prefect.context.get("logger")
    logger.info(f"Uploading grade for submission {subm['name']}")
    if subm['score'] is not None:
        raise signals.SKIP("Grade already uploaded.")

    logger.info(f"Obtaining score from the gradebook")
    try:
        gb = Gradebook('sqlite:///'+os.path.join(subm['grader']['folder'] , 'gradebook.db'))
        gb_subm = gb.find_submission(subm['assignment']['name'], config.grading_student_folder_prefix+subm['student']['id'])
        score = gb_subm.score
    except Exception as e:
        sig = signals.FAIL(f"Error when accessing the gradebook score for submission {subm['name']}; error {str(e)}")
        sig.e = e
        sig.subm = subm
        raise sig
    finally:
        gb.close()
    logger.info(f"Score: {score}")

    logger.info(f"Computing the max score from the release notebook")
    try:
        max_score = _compute_max_score(config, subm)
    except Exception as e:
        sig = signals.FAIL(f"Error when trying to compute the max score for submission {subm['name']}; error {str(e)}")
        sig.e = e
        sig.subm = subm
        raise sig
    logger.info(f"Max Score: {max_score}")

    self.score = score
    self.max_score = max_score
    pct = "{:.2f}".format(100*score/max_score)
    logger.info(f"Percent grade: {pct}")

    logger.info(f"Uploading to Canvas...")
    subm['score'] = pct
    put_grade(config, subm)

    return subm


