import os
import re
import datetime
import constants.globals as globals





class truthabout:

    def __init__(self):
        self.truthname = ''
        self.truth_config = []
        self.job_filename = ''
        selfjob_schedulefilename = ''
        self._stage_type = {
            "ParquetExtract": self.stage_parquet_extract,
            # "SQLTransform": self.write_create_table_sql_block,
            # "ParquetLoad": self.write_load_parquet,
            # "JDBCExecute": self.write_jdbc_excute,
            # "JDBCLoad": self.write_jdbc_load,
            "Schedule": self.write_schedule_file

        }


    #setters
    def set_truthname(self,truthname):
        self.truthname = truthname


    def set_truth_config(self,truth_config):
        self.truth_config = truth_config

    def set_job_filename(self,filename):
        self.job_filename = filename

    #getters
    def get_truthname(self):
        return self.truthname


    def get_truth_config(self):
        return self.truth_config

    ######################
    #  write job and schedule file
    def write_files(self):
        try:
            self.write_job_file()
            self.write_schedule_file()
        except ValueError as err:
            print("Error: ", err)
        else:
            print("Job and schedule files created")

    ##################
    # File reader:
    def read_config_file(self,truthname,config_path):
        config_filename = os.path.join(os.path.normpath(os.getcwd()), (config_path + truthname + "_config.txt"))
        try:
            # file exists proceed further,split each line to array element
            with open(config_filename) as file:
                print("Reading constants file : " + config_filename)
                input_array = []
                for line in file:
                    input_array.append(list(filter(None, re.split("[,]+", line.strip()))))
            file.close()
            return self.clean_config_array(truthname, input_array)
        except FileNotFoundError as e:
            print(e)
            exit(1)

    # Input cleaner:
    def clean_config_array(self,truthname, config_array):
        print("------------------------------- " + truthname + "_config.txt -------------------------------")
        for line in range(len(config_array)):
            for task in range(len(config_array[line])):
                config_array[line][task] = config_array[line][task].strip()
            print(config_array[line])
        print("------------------------------- " + truthname + "_config.txt -------------------------------")
        return config_array



    #write the common block
    def write_block_common(self):
        template_file = os.path.join(os.path.normpath(os.getcwd()), (globals.TEMPLATES_PATH + "common_block.txt"))
        content = ""
        try:
            with open(template_file,'r',encoding='unicode_escape') as infile:
                content = infile.read()
                content = re.sub("<<sql_root>>" , globals.NAMINGCONV["sql_root"],content)
                content = re.sub("<<short_prefix>>",globals.NAMINGCONV["short_prefix"],content)
                content = re.sub("<<truthname>>",self.truthname,content)
                content = re.sub("<<JOB_ENVIRONS>>",globals.JOB_ENVIRONS,content)
        except FileNotFoundError as err:
            print("ERROR write_block_common: " ,err)

        else:
            return(content)

    #########################
    # write block parquet extract



    def write_block_parquet_extract(self,data_lake_location,table_name):
        # table_name = args[0]  #"<<table_name>>",
        # data_lake_location = args[1] #<<data_lake_location>>
        template_file = os.path.join(os.path.normpath(os.getcwd()), (globals.TEMPLATES_PATH + "parquet_extract_block.txt"))
        content = ""
        try:
            with open(template_file, 'r', encoding='unicode_escape') as infile:
                content = infile.read()
                content = re.sub("<<table_name>>", table_name, content)
                content = re.sub("<data_lake_location>>", data_lake_location, content)
        except FileNotFoundError as err:
            print(err, " while trying to create _job.json for parquet extract")

        else:
            return (content)


    #############################

    # write to file
    def write_block_tail(self):
        pass


    # write to file
    def write_to_file(self,content):
        with open(self.job_filename, 'a') as outfile:
            #block_to_write = self.write_block_common()
            #outfile.write(block_to_write)
            outfile.write(content)
        outfile.close()

    # write schedule_file
    def create_schedule_file(self, filename, content):
        with open(filename, 'w') as outfile:
            #block_to_write = self.write_block_common()
            outfile.write(content)
        outfile.close()
        self.truth_schedule = filename


    ####################
    ## write job file ##


    def write_job_file(self):
        file_path = self.get_file_path()
        filename = os.path.join(file_path, (self.truthname + "_job.json"))
        print("job file path is: ", filename)
        self.set_job_filename(filename)
        try:
            # write the common block
            #hard delete the job file
            self.write_to_file(self.write_block_common())

            #write stages
            self.write_block_stage()
        except ValueError as err:
            print("Error: " ,err)

    #######################


    # def do_stage(self, arg,args):
    #     #return getattr(self, arg)()
    #     #print(self._stage_type[arg])
    #     #return self._stage_type[arg](*arg,**kwargs)
    #     return(self._stage_type[arg])(args)

    ############################

    #write the stages of the json file
    def write_block_stage(self):
        # Reading through lines in in_arr:
        print("<<<<<<<<<<<< stages in the config file are >>>>>>>>>>>")
        count = 0
        for stage in self.truth_config:
            if stage[0] not in self._stage_type:
                print("Error PE1: Invalid stage type: ",stage[0])
            else:
                if count != len(self.truth_config):

                    #self.do_stage(self._stage_type[stage[0]],stage[1:])
                    self._stage_type[stage[0]](stage[1:])


                    # strip comma fromt the last block
                    # better to remove from last than add to each block or strip it when adding the tail
                    pass


        print("<<<<<<<<<<<<        >>>>>>>>>>>")

    #######################
    # write parquet stage
    def stage_parquet_extract(self,args):
        # table_name = args[0]  #"<<table_name>>",
        # data_lake_location = args[1] #<< data_lake_location >>
        print("stage_parquet_extract: ",args)
        self.write_to_file(self.write_block_parquet_extract(args[0],args[1]))






    ################

    def write_create_table_sql_block(self):
        pass

    def write_load_parquet(self):
        pass

    def write_jdbc_excute(self):
        pass

    def write_jdbc_load(self):
        pass



    ##############################
    ##############################
    def write_schedule_file(self,line = []):
        template_file = os.path.join(os.path.normpath(os.getcwd()), (globals.TEMPLATES_PATH + "schedule_block.txt"))
        line = self.truth_config[0]
        print(line, template_file)
        print(len(line))

        if len(line) not in [2, 5]:
            print('ERROR1: invalid config file')
            return False

        elif line[0] != 'Schedule':
            print('ERROR2: invalid schedule stage_type in config file')
            return False
        elif len(line) == 2:
            if self.schedule_job_now(line[1]):
                print("scheduled now")
                return True
        else:
            if self.schedule_job_future(line[1:]):
                print("scheduled for future")
                return True

    # replace content for schedule file
    def replace_schedule_file(self,size, start_time=globals.DEFAULT_START_TIME, interval=globals.DEFAULT_INTERVAL):
        content = ''
        template_file = os.path.join(os.path.normpath(os.getcwd()), (globals.TEMPLATES_PATH + "schedule_block.txt"))
        try:
            with open(template_file, 'r', encoding='unicode_escape') as infile:
                content = infile.read()
                content = re.sub("<<size>>", size, content)
                content = re.sub("<<start_time>>", start_time, content)
                content = re.sub("<<interval>>", interval, content)
        except FileNotFoundError as err:
            print(err, " while trying to create _schedule.json")
        else:
            return (content)

    ########################
    # schedule_job_now(vm_size)
    def schedule_job_now(self,size):
        if size not in globals.SIZE_OPTIONS:
            print("ERROR3: invalid job size ")
            return False
        else:
            content = self.replace_schedule_file(size)
            file_path = self.get_file_path()
            print("file path is: " ,file_path)
            filename = os.path.join(file_path ,(self.truthname +"_schedule.json"))
            self.create_schedule_file(filename,content)
    ##############################

    ##############################


    def get_file_path(self):
        if os.path.exists(os.path.join(os.path.normpath(os.getcwd()), (globals.WRITE_OUTPUT_FILE + self.truthname))):
            return ((os.path.join(os.path.normpath(os.getcwd()), (globals.WRITE_OUTPUT_FILE + self.truthname))))
        else:
            return (os.mkdir(os.path.join(os.path.normpath(os.getcwd()), (globals.WRITE_OUTPUT_FILE + self.truthname))))


    ##############################
    def schedule_job_future(self,schedule):
        date,time,interval,size = schedule
        if date == '' or time == '' or interval== '' :
            print('invalid config file: cannot have an null start date & time or interval for a future job')
            return False
        else:
            print("schedule for future")
            flag = self.check_valid_future_date_time(date,time)
            if flag:
                flag = self.check_valid_interval(interval)
                #write to schedule fine now
                day,month,year = date.split('/')
                start_date = '/'.join([year,month,day]) # framework expect yyy/mm/day hh:mm:ss format
                content = self.replace_schedule_file(size,' '.join([start_date,time]),interval)
                file_path = self.get_file_path()
                print("file path is: ", file_path)
                filename = os.path.join(file_path, (self.truthname + "_schedule.json"))
                self.create_schedule_file(filename, content)

            else:
                return False

    #### check valid date
    def check_valid_future_date_time(self,date,time):
        #date format : dd/mm/yyyy
        day, month, year = date.split('/')
        flag = True
        try:
            schedule_date = datetime.datetime(int(year), int(month), int(day))
        except ValueError:
            flag = False
            print("ERROR: invalid date")
        else:
            if not (schedule_date >= datetime.datetime.now()):
                print("ERROR: Schedule date cannot be in the past")
                flag = False
            else:
                if schedule_date == str(datetime.datetime.now()):
                    flag = self.check_valid_time(time,True)
                else:
                    flag = self.check_valid_time(time)

        return flag


    ############################
    def check_valid_time(self,schedule_time,schedule_today = False):
        # time format : hh:mm:ss
        hours,minutes,seconds = schedule_time.split(':')

        try:
            schedule_time = datetime.time(int(hours),int(minutes),int(seconds))
        except ValueError:
            print("ERROR: invalid date")
            return False

        if schedule_today:
            # schedule start time should be atleast an hour from now,considering batcha nd build delays
            min_delta = str(datetime.datetime.now().time()) + str(datetime.timedelta(seconds=3600))
            print(datetime.datetime.now() ) # - datetime.timedelta(seconds=60))
            if str(schedule_time) <  min_delta:
                print("ERROR: Schedule time to be atleast an hour from now")
                return False
        else:
            return True

    #########################
    def check_valid_interval(self,interval):
        if not (type(interval) == 'int') or int(interval) < 60:
            # repeat cycle to be min 60 mins due to batch spin and build time
            return False
        else:
            return True


    #################################################################
    ##### Not needed for bulk load/can be changed later #############
    #################################################################
    # -------------------------------------- _sql_exec_stored_procedure.sql: --------------------------------------
    # Write exec_stored_proc
    def w_exec_stored_proc(truthname, classification):
        pass

