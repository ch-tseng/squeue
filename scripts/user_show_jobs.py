from libQueue import SQ

id = input('Job ID: ')
qhost = SQ()
print('')
print('')
qhost.get_job_content(id)
