import datetime
import os

def get_file_path(self):
    if os.dir(os.path.join(os.path.normpath(os.getcwd()), (globals.WRITE_OUTPUT_FILE + self.truthname))):
        return ((os.path.join(os.path.normpath(os.getcwd()), (globals.WRITE_OUTPUT_FILE + self.truthname))))

    else:
        return (os.mkdir(os.path.join(os.path.normpath(os.getcwd()), (globals.WRITE_OUTPUT_FILE + self.truthname))))

        ##############################