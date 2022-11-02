import os
import sys
import yaml
from prefect import flow
# from prefect.flow_runners.subprocess import SubprocessFlowRunner
# from prefect.blocks.storage import TempStorageBlock
from prefect.client import get_client
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule

from rudaux.model import Settings
from rudaux.task.autoext import compute_autoextension_override_updates
from rudaux.tasks import get_learning_management_system, get_grading_system, get_submission_system
from rudaux.task.learning_management_system import get_students, get_assignments, get_submissions, \
    get_course_info, update_override, create_overrides, delete_overrides

from rudaux.task.snap import get_pastdue_snapshots, get_existing_snapshots, \
    get_snapshots_to_take, take_snapshots, verify_snapshots


# -------------------------------------------------------------------------------------------------------------
def load_settings(path):
    # load settings from the config
    print(f"Loading the rudaux configuration file {path}...")
    if not os.path.exists(path):
        sys.exit(
            f"""
              There is no configuration file at {path},
              and no other file was specified on the command line. Please
              specify a valid configuration file path.
              """
        )
    else:
        with open(path) as f:
            config = yaml.safe_load(f)

    return Settings.parse_obj(obj=config)


# -------------------------------------------------------------------------------------------------------------
async def run(args):
    # load settings from the config
    settings = load_settings(args.config_path)
    # start the prefect agent
    os.system(f"prefect agent start --work-queue {settings.prefect_queue_name}")


# -------------------------------------------------------------------------------------------------------------
async def register(args):
    # load settings from the config
    settings = load_settings(args.config_path)

    # start the client
    async with get_client() as client:
        # remove old rudaux deployments
        current_deployments = await client.read_deployments()
        for deployment in current_deployments:
            if settings.prefect_deployment_prefix in deployment.name:
                await client.delete_deployment(deployment.id)

        deployment_ids = []

        per_section_flows = [
            # (autoext_flow, settings.autoext_prefix, settings.autoext_cron_string),
            (snap_flow, settings.snap_prefix, settings.snap_cron_string)
        ]

        per_course_flows = [
            # (grade_flow, settings.grade_prefix, settings.grade_cron_string),
            # (soln_flow, settings.soln_prefix, settings.soln_cron_string),
            # (fdbk_flow, settings.fdbk_prefix, settings.fdbk_cron_string)
        ]

        for course_name in settings.course_groups:
            # -------------------------------------------------------------------------------------------------
            # building deployments for course groups
            for fl, prefix, cron in per_course_flows:
                name = settings.prefect_deployment_prefix + prefix + course_name

                deployment = await Deployment.build_from_flow(
                    flow=fl,
                    name=name,
                    work_queue_name=settings.prefect_queue_name,
                    schedule=CronSchedule(cron=cron, timezone="America/Vancouver"),
                    parameters={'settings': settings.dict(), 'course_name': course_name},
                )
                print(deployment)
                print('\n')
                deployment_id = await deployment.apply()
                deployment_ids.append(deployment_id)

            # -------------------------------------------------------------------------------------------------
            # building deployments for course sections
            for section_name in settings.course_groups[course_name]:
                for fl, prefix, cron in per_section_flows:
                    name = settings.prefect_deployment_prefix + prefix + section_name

                    deployment = await Deployment.build_from_flow(
                        flow=fl,
                        name=name,
                        work_queue_name=settings.prefect_queue_name,
                        schedule=CronSchedule(cron=cron, timezone="America/Vancouver"),
                        parameters={'settings': settings.dict(),
                                    'course_name': course_name, 'section_name': section_name},
                    )
                    print(deployment)
                    print('\n')
                    deployment_id = await deployment.apply()
                    deployment_ids.append(deployment_id)

            # -------------------------------------------------------------------------------------------------
        print("Flows registered.")
    return


# -------------------------------------------------------------------------------------------------------------
@flow
def autoext_flow(settings: dict, course_name: str, section_name: str) -> None:
    """
    applies extension overrides for certain students

    Parameters
    ----------
    settings: dict
    course_name: str
    section_name: str
    """

    # settings object was serialized by prefect when registering the flow, so need to reparse it
    settings = Settings.parse_obj(settings)

    # Create an LMS object
    lms = get_learning_management_system(settings=settings, group_name=course_name)

    # Get course info, list of students, and list of assignments from lms
    course_info = get_course_info(lms=lms, course_section_name=section_name)
    students = get_students(lms=lms, course_section_name=section_name)
    assignments = get_assignments(lms=lms, course_group_name=course_name,
                                  course_section_name=section_name)

    # Compute the set of overrides to delete and new ones to create
    # we formulate override updates as delete first, wait, then create to avoid concurrency issues
    # TODO map over assignments here (still fine with concurrency)

    # compute the set of overrides to delete and new ones to create for all assignments
    overrides = compute_autoextension_override_updates(
        settings=settings, course_name=course_name, section_name=section_name,
        course_info=course_info, students=students, assignments=assignments)

    # for each assignment remove the old overrides and create new ones
    for assignment, overrides_to_create, overrides_to_delete in overrides:
        # if overrides_to_delete is not None:
        delete_response = delete_overrides(lms=lms, course_section_name=section_name,
                                           assignment=assignment, overrides=overrides_to_delete)

        # if overrides_to_create is not None:
        create_response = create_overrides(lms=lms, course_section_name=section_name,
                                           assignment=assignment, overrides=overrides_to_create,
                                           wait_for=[delete_response])


# -------------------------------------------------------------------------------------------------------------
@flow
def snap_flow(settings: dict, course_name: str, section_name: str) -> None:
    """
    does the following;
    - computes the snapshots to be taken
    - gets all existing snapshots and identifies the ones which are not already taken and need to be taken
    - validates whether the snapshots to be taken were in fact taken

    Parameters
    ----------
    settings: dict
    course_name: str
    section_name: str
    """

    # settings object was serialized by prefect when registering the flow, so need to reparse it
    settings = Settings.parse_obj(settings)

    # Create an LMS and SubS object
    lms = get_learning_management_system(settings=settings, group_name=course_name)
    subs = get_submission_system(settings=settings, group_name=course_name)

    # initiate the submission system (open ssh connection)
    subs.open()

    # Get course info, list of students, and list of assignments from lms
    course_info = get_course_info(lms=lms, course_section_name=section_name)
    students = get_students(lms=lms, course_section_name=section_name)
    assignments = get_assignments(lms=lms, course_group_name=course_name, course_section_name=section_name)
    # submissions = get_submissions(lms=lms, course_group_name=course_name, course_section_name=section_name,
    #                               assignment=settings.assignments[course_name])

    # get list of snapshots past their due date from assignments
    pastdue_snaps = get_pastdue_snapshots(
        course_name=course_name, course_info=course_info, assignments=assignments)

    # get list of existing snapshots from submission system
    existing_snaps = get_existing_snapshots(assignments=assignments, students=students, subs=subs)

    # compute snapshots to take
    snaps_to_take = get_snapshots_to_take(pastdue_snaps=pastdue_snaps, existing_snaps=existing_snaps)

    # take snapshots
    take_snapshots(snaps_to_take=snaps_to_take, subs=subs)

    # get list of newly existing snapshots from submission system
    new_existing_snaps = get_existing_snapshots(assignments=assignments, students=students, subs=subs)

    # verify snapshots
    verify_snapshots(snaps_to_take=snaps_to_take, new_existing_snaps=new_existing_snaps)

    subs.close()


# -------------------------------------------------------------------------------------------------------------
@flow
def grade_flow(settings: dict, course_name: str):
    settings = Settings.parse_obj(settings)

    # Create an LMS, SubS, and GradS objects
    lms = get_learning_management_system(settings=settings, group_name=course_name)
    subs = get_submission_system(settings=settings, group_name=course_name)
    grds = get_grading_system(settings=settings, group_name=course_name)

    course_section_names = settings.course_groups[course_name]
    selected_assignments = settings.assignments[course_name]

    for section_name in course_section_names:

        # Get course info, list of students, and list of assignments from lms
        course_info = get_course_info(lms=lms, course_section_name=section_name)
        students = get_students(lms=lms, course_section_name=section_name)
        assignments = get_assignments(lms=lms, course_group_name=course_name, course_section_name=section_name)

        for assignment_id, assignment in assignments.items():
            if assignment.name in selected_assignments:
                submissions = get_submissions(
                    lms=lms, course_group_name=course_name,
                    course_section_name=section_name, assignment=assignment
                )


# -------------------------------------------------------------------------------------------------------------
@flow
def soln_flow(settings: dict, course_name: str):
    settings = Settings.parse_obj(settings)


# -------------------------------------------------------------------------------------------------------------
@flow
def fdbk_flow(settings: dict, course_name: str):
    settings = Settings.parse_obj(settings)


# -------------------------------------------------------------------------------------------------------------
async def list_course_info(args):
    # load settings from the config
    settings = load_settings(args.config_path)
    for course_name in settings.course_groups:
        lms = get_learning_management_system(settings, course_name)
        pass  # TODO

    # asgns = []
    # studs = []
    # tas = []
    # insts = []
    # for group in config.course_groups:
    #    for course_id in config.course_groups[group]:
    #        section_name = config.section_names[course_id]
    #        asgns.extend([(section_name, a) for a in api._canvas_get(config, course_id, 'assignments')])
    #        studs.extend([(section_name, s) for s in api._canvas_get_people_by_type(config, course_id, 'StudentEnrollment')])
    #        tas.extend([(section_name, s) for s in api._canvas_get_people_by_type(config, course_id, 'TaEnrollment')])
    #        insts.extend([(section_name, s) for s in api._canvas_get_people_by_type(config, course_id, 'TeacherEnrollment')])
    # print()
    # print('Assignments')
    # print()
    # print('\n'.join([f"{c[0] : <16}{c[1]['name'] : <32}{c[1]['id'] : <16}" for c in asgns]))

    # print()
    # print('Students')
    # print()
    # print('\n'.join([f"{c[0] : <16}{c[1]['name'] : <32}{c[1]['id'] : <16}{str(c[1]['reg_date'].in_timezone(config.notify_timezone)) : <32}{c[1]['status'] : <16}" for c in studs]))

    # print()
    # print('Teaching Assistants')
    # print()
    # print('\n'.join([f"{c[0] : <16}{c[1]['name'] : <32}{c[1]['id'] : <16}{str(c[1]['reg_date'].in_timezone(config.notify_timezone)) : <32}{c[1]['status'] : <16}" for c in tas]))

    # print()
    # print('Instructors')
    # print()
    # print('\n'.join([f"{c[0] : <16}{c[1]['name'] : <32}{c[1]['id'] : <16}{str(c[1]['reg_date'].in_timezone(config.notify_timezone)) : <32}{c[1]['status'] : <16}" for c in insts]))

# def fail_handler_gen(config):
#    def fail_handler(flow, state, ref_task_states):
#        if state.is_failed():
#            sm = ntfy.SendMail(config)
#            sm.notify(config.instructor_user, f"Hi Instructor, \r\n Flow failed!\r\n Message:\r\n{state.message}")
#    return fail_handler
#

# def build_autoext_flows(config):
#    """
#    Build the flow for the auto-extension of assignments for students
#    who register late.
#
#    Params
#    ------
#    config: traitlets.config.loader.Config
#        a dictionary-like object containing the configurations
#        from rudaux_config.py
#    """
#    flows = []
#    for group in config.course_groups:
#        for course_id in config.course_groups[group]:
#            with Flow(config.section_names[course_id] + "-autoext",
#                      terminal_state_handler=fail_handler_gen(config)) as flow:
#
#                assignment_names = list(config.assignments[group].keys())
#
#                # Obtain course/student/assignment/etc info from the course API
#                course_info = api.get_course_info(config, course_id)
#                assignments = api.get_assignments(config, course_id, assignment_names)
#                students = api.get_students(config, course_id)
#                submission_info = combine_dictionaries(api.get_submissions.map(unmapped(config), unmapped(course_id), assignments))
#
#                # Create submissions
#                submission_sets = subm.initialize_submission_sets(config, [course_info], [assignments], [students], [submission_info])
#
#                # Fill in submission deadlines
#                submission_sets = subm.build_submission_set.map(unmapped(config), submission_sets)
#
#                # Compute override updates
#                overrides = subm.get_latereg_overrides.map(unmapped(config.latereg_extension_days[group]), submission_sets, unmapped(config))
#
#                # TODO: we would ideally do flatten(overrides) and then
#                # api.update_override.map(unmapped(config), unmapped(course_id), flatten(overrides))
#                # but that will cause prefect to fail. see https://github.com/PrefectHQ/prefect/issues/4084
#                # so instead we will code a temporary hack for update_override.
#                api.update_override_flatten.map(unmapped(config), unmapped(course_id), overrides)
#
#            flows.append(flow)
#    return flows
#
#
#
#
# def snapshot_flow(config):
#    interval = config.snapshot_interval
#    while True:
#        for group in config.course_groups:
#            # Create an LMS API connection
#            lms = LMSAPI(group, config)
#
#            # Create a Student API connection
#            stu = StudentAPI(group, config)
#
#            # Obtain assignments from the course API
#            assignments = lms.get_assignments()
#
#            # obtain the list of existing snapshots
#            existing_snaps = stu.get_snapshots()
#
#            # compute snapshots to take
#            snaps_to_take = stu.compute_snaps_to_take(assignments, existing_snaps)
#
#            # take new snapshots
#            stu.take_snapshots(snaps_to_take)
#
#        # sleep until the next run time
#        print(f"Snapshot waiting {interval} minutes for next run...")
#        time.sleep(interval*60)
#    # end func
#
## this creates one flow per grading group,
## not one flow per assignment. In the future we might
## not load assignments/graders from the rudaux config but
## rather dynamically from LMS; there we dont know what
## assignments there are until runtime. So doing it by group is the
## right strategy.
# def grading_flow(config):
#    try:
#        check_output(['sudo', '-n', 'true'])
#    except CalledProcessError as e:
#        assert False, f"You must have sudo permissions to run the flow. Command: {e.cmd}. returncode: {e.returncode}. output {e.output}. stdout {e.stdout}. stderr {e.stderr}"
#    hour = config.grade_hour
#    minute = config.grade_minute
#    while True:
#        # wait for next grading run
#        t = plm.now().in_tz(config.grading_timezone)
#        print(f"Time now: {t}")
#        tgrd = plm.now().at(hour = hour, minute = minute)
#        if t > tgrd:
#            tgrd = tgrd.add(days=1)
#        print(f"Next grading flow run: {tgrd}")
#        print(f"Grading waiting {(tgrd-t).total_hours()} hours for run...")
#        time.sleep((tgrd-t).total_seconds())
#
#        # start grading run
#        for group in config.course_groups:
#            # get the course names in this group
#            section_names = config.course_groups[group]
#            # create connections to APIs
#            lsapis = {}
#            for section_name in section_names:
#                lsapis[section_name]['lms'] = LMSAPI(section_name, config)
#                lsapis[section_name]['stu'] = StudentAPI(section_name, config)
#            # Create a Grader API connection
#            grd = GraderAPI(group, config)
#
#
#            # Obtain course/student/assignment/etc info from the course API
#            course_infos = api.get_course_info.map(unmapped(config), course_ids)
#            assignment_lists = api.get_assignments.map(unmapped(config), course_ids, unmapped(assignment_names))
#            student_lists = api.get_students.map(unmapped(config), course_ids)
#            submission_infos = []
#            for i in range(len(course_ids)):
#                submission_infos.append(combine_dictionaries(api.get_submissions.map(unmapped(config), unmapped(course_ids[i]), assignment_lists[i])))
#
#            # Create submissions
#            submission_sets = subm.initialize_submission_sets(unmapped(config), course_infos, assignment_lists, student_lists, submission_infos)
#
#            # Fill in submission details
#            submission_sets = subm.build_submission_set.map(unmapped(config), submission_sets)
#
#            # Create grader teams
#            grader_teams = grd.build_grading_team.map(unmapped(config), unmapped(group), submission_sets)
#
#            # create grader volumes, add git repos, create folder structures, initialize nbgrader
#            grader_teams = grd.initialize_volumes.map(unmapped(config), grader_teams)
#
#            # create grader jhub accounts
#            grader_teams = grd.initialize_accounts.map(unmapped(config), grader_teams)
#
#            # assign graders
#            submission_sets = subm.assign_graders.map(unmapped(config), submission_sets, grader_teams)
#
#            # compute the fraction of submissions past due for each assignment,
#            # and then return solutions for all assignments past the threshold
#            pastdue_fracs = subm.get_pastdue_fraction.map(submission_sets)
#            subm.return_solutions.map(unmapped(config), pastdue_fracs, submission_sets)
#
#            ## collect submissions
#            submission_sets = subm.collect_submissions.map(unmapped(config), submission_sets)
#
#            ## clean submissions
#            submission_sets = subm.clean_submissions.map(submission_sets)
#
#            ## Autograde submissions
#            submission_sets = subm.autograde.map(unmapped(config), submission_sets)
#
#            ## Wait for manual grading
#            submission_sets = subm.check_manual_grading.map(unmapped(config), submission_sets)
#
#            ## Collect grading notifications
#            grading_notifications = subm.collect_grading_notifications.map(submission_sets)
#
#            ## Skip assignments with incomplete manual grading
#            submission_sets = subm.await_completion.map(submission_sets)
#
#            ## generate & return feedback
#            submission_sets = subm.generate_feedback.map(unmapped(config), submission_sets)
#            subm.return_feedback.map(unmapped(config), pastdue_fracs, submission_sets)
#
#            ## Upload grades
#            submission_sets = subm.upload_grades.map(unmapped(config), submission_sets)
#
#            ## collect posting notifications
#            posting_notifications = subm.collect_posting_notifications.map(submission_sets)
#
#            ## send notifications
#            grading_notifications = filter_skip(grading_notifications)
#            posting_notifications = filter_skip(posting_notifications)
#            ntfy.notify(config, grading_notifications, posting_notifications)
#        # end while
#    # end func
#
## TODO a flow that resets an assignment; take in parameter, no interval,
## require manual task "do you really want to do this"
# def build_reset_flow(_config, args):
#    raise NotImplementedError
#
# def status(args):
#    print(f"Creating the {__PROJECT_NAME} client...")
#    client = prefect.client.client.Client()
#
#    # TODO this function currently just contains a bunch of (functional)
#    # test code. need to turn this into a func that prints status etc
#
#    #client.get_flow_run_info(flow_run_id)
#    #client.get_task_run_info(flow_run_id, task_id, map_index = ...)
#    #client.get_flow_run_state(flow_run_id)
#    #client.get_task_run_state(task_run_id)
#
#    print("Querying for flows...")
#    query_args = {}
#    flow_query = {
#        "query": {
#            "flow" : {
#                "id": True,
#                "settings": True,
#                "run_config": True,
#                "serialized_flow": True,
#                "name": True,
#                "archived": True,
#                "project": {"name"},
#                "core_version": True,
#                "storage": True,
#                "flow_group": {"labels"},
#            }
#        }
#    }
#    result = client.graphql(flow_query)
#    flows = result.get("data", {}).get("flow", None)
#
#    for flow in flows:
#        print(FlowView.from_flow_id(flow['id']))
#
#    flow_run_query = {
#        "query": {
#             "flow_run" : {
#                "id": True,
#                "name": True,
#                "flow_id": True,
#                "serialized_state": True,
#                "states": {"timestamp", "serialized_state"},
#                "labels": True,
#                "parameters": True,
#                "context": True,
#                "updated": True,
#                "run_config": True,
#            }
#        }
#    }
#    result = client.graphql(flow_run_query)
#    flowruns = result.get("data", {}).get("flow_run", None)
#    for flowrun in flowruns:
#        print(FlowRunView.from_flow_run_id(flowrun['id']))
#
