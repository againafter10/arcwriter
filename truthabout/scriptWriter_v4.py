import truthabout as ta
from constants import globals as globals
import os

try:
    truthname = input("Truthname: ")
    if not truthname:
        raise ValueError('empty string')
except ValueError as err:
    print(err)
    print("Found null truth name,quitting...")
    quit()

truth = ta.truthabout()
truth.set_truthname(truthname)
truth_config = truth.read_config_file(truth.truthname,globals.CONFIGS_PATH)
truth.set_truth_config(truth_config)

#set the class variables(vm size ,list if tasks,list of sql etc.

#create the _job.json from the input constants file
#file_path = truth.get_file_path()
#print("file path is: " ,file_path)
#job_file = os.path.join(file_path ,(truth.truthname +"_schedule.json"))
#base_path= "".join([(os.path.normpath(os.getcwd())),"/truthabout/"])
#job_file = base_path + globals.JOBS_PATH + globals.NAMINGCONV["truth_prefix"] + truth.truthname + "_job.json"
#template_file = os.path.join(os.path.normpath(os.getcwd()), (globals.TEMPLATES_PATH))
#print(job_file)

#truth.write_to_file(job_file,truth.write_block_common())
#truth.write_to_file(job_file,truth.write_block_stage())

# truth.write_job_file()
#should be the last
#truth.write_schedule_file(truth_config[0])
# truth.write_schedule_file()
truth.write_files()









#exit = input("Press <enter> to close...")

