import rudaux

c.name = 'dsci100'
c.canvas_domain = 'https://canvas.ubc.ca'
c.canvas_id = '12345' #course number from the canvas URL
c.canvas_token = '23487~sdfasdfga3847fga874fga8347fgaf' #canvas API token (this example was generated by the ISmashedMyKeyboard algorithm)
c.user_folder_root = '/tank/home/dsci100' #the root folder for users on *both* student and instructor jupyterhub servers
c.student_local_assignment_folder = 'dsci-100/materials' # the name of the student repository and the subdirectory in the students repository where assignments are stored (if it is used)
c.grading_image = 'yourdockeraccount/your-docker-image:v0.1'
c.jupyterhub_host_root = 'your-student-jupyterhub.domain.com'
c.jupyterhub_config_dir = '/srv/jupyterhub/' #the folder where jupyterhub_config and zfs_homedir.sh is
c.latereg_extension_days = 7 #number of days to give extensions for late registrations (registration date + 7 days here)
c.instructor_user = 'your_username' #your username on the jupyterhub (you have to create this using dictauth)
c.instructor_repo_url = 'git@github.com:your-account/your-repo.git' #the git url for the course material
c.return_solution_threshold = 0.93 #the fraction of students whose assignments must be collected before you return solutions
c.student_folder_root = '/tank-student/home/dsci100' #the NFS mount point on the instructor jupyterhub server for /tank/home/dsci100 from student server
c.num_docker_threads = 4 #the number of CPU threads to use when grading, generating feedback, etc
c.earliest_solution_return_date = '2020-10-02 01:00:00' #the earliest date in the course to return any solutions for anything

c.notify_days = ['Monday', 'Thursday'] #days of the week to send grading reminder emails to graders (emails are sent to instructor for any errors any day)
c.notification_type = rudaux.notification.SendMail #use this for local email server sending (no account required); use rudaux.notification.SMTP for remote smtp server
c.sendmail.address = 'dscibot@domain.com' #the "from" address for your notifications
c.sendmail.contact_info = {  #contact info for you and graders -- format is 'your_jupyterhub_username' : {'name' : 'Your Nice Name', 'address' : 'your.email@email.com'}
   'your_username' : {'name' : 'Your Nice Name', 'address' : 'your.email@email.com'},
   'a_ta_name' : {'name' : 'TA Nice Name', 'address' : 'ta.email@email.com'},
   'a_ta_name' : {'name' : 'TA Nice Name', 'address' : 'ta.email@email.com'}
}
#don't need this unless using notification_type = rudaux.notification.SMTP
#c.smtp.hostname = 'smtp.otherdomain.com:587'
#c.smtp.address = 'dsci100bot@otherdomain.com'
#c.smtp.username = 'dsci100bot'
#c.smtp.passwd = 'your_password'
#c.smtp.contact_info = {  #contact info for you and graders -- format is 'your_jupyterhub_username' : {'name' : 'Your Nice Name', 'address' : 'your.email@email.com'}
#   'your_username' : {'name' : 'Your Nice Name', 'address' : 'your.email@email.com'},
#   'a_ta_name' : {'name' : 'TA Nice Name', 'address' : 'ta.email@email.com'},
#   'a_ta_name' : {'name' : 'TA Nice Name', 'address' : 'ta.email@email.com'}
#}

c.graders = { #the list of graders for each jupyterhub assignment
    'worksheet_01' : ['your_username'],
    'worksheet_02' : ['your_username'],
    'tutorial_01' : ['ta_username', 'other_ta_username'],
    'tutorial_02' : ['ta_username', 'other_ta_username']
}
