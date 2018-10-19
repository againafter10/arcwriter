import truthabout as ta
from constants import globals as globals

try:
    truthname = input("Truthname: ")
    if not truthname:
        raise ValueError('empty string')
except ValueError as err:
    print(err)
    print("Found null truth name,quitting...")
    quit(1)

truth = ta.truthabout()
truth.set_truthname(truthname)
truth_config = truth.read_config_file(truth.truthname,globals.CONFIGS_PATH)
truth.set_truth_config(truth_config)
truth.write_files()

