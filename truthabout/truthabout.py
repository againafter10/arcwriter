import os
import re
import datetime
import constants.globals as globals

class truthabout:

    def __init__(self):
        self.truthname = ''
        self.truth_config = []
        self.job_filename = ''
        self.schedule_filename = ''
        self._stage_type = {
            "ParquetExtract": self.stage_parquet_extract,
            "SQLTransform": self.stage_sql_transform,
            "ParquetLoad": self.stage_parquet_load,
            "JDBCExecute": self.stage_jdbc_execute,
            "JDBCLoad": self.stage_jdbc_load,
            "Schedule": self.write_schedule_file

        }

    #setters
    def set_truthname(self, truthname):
        self.truthname = truthname

    def set_truth_config(self, truth_config):
        self.truth_config = truth_config

    def set_job_filename(self, job_filename):
        self.job_filename = job_filename

    def set_job_schedule(self, schedule_filename):
        self.set_job_schedulename = schedule_filename

    #getters
    def get_truthname(self):
        return self.truthname

    def get_truth_config(self):
        return self.truth_config

    def get_job_filename(self, job_filename):
       return self.job_filename

    def get_job_schedule(self, schedule_filename):
        return self.schedule_filename

    # ------------------------
    # File reader:
    def read_config_file(self,truthname, config_path):
        config_filename = os.path.join(os.path.normpath(os.getcwd()), (config_path + truthname + "_config.txt"))
        try:
            # file exists proceed further,split each line to array element
            with open(config_filename) as file:
                print("\nReading constants file : " + config_filename)
                input_array = []
                for line in file:
                    input_array.append(list(filter(None, re.split("[,]+", line.strip()))))
            file.close()
            return self.clean_config_array(truthname, input_array)
        except FileNotFoundError as e:
            print(e)
            exit(1)

    # ------------------------
    # Input cleaner:
    def clean_config_array(self,truthname, config_array):
        for line in range(len(config_array)):
            for task in range(len(config_array[line])):
                config_array[line][task] = config_array[line][task].strip()
        return config_array

    # ------------------------
    #  write job and schedule file
    def write_files(self):
        try:
            self.write_job_file()
            self.write_schedule_file()
        except ValueError as err:
            print("Error: ", err)
        else:
            print("\n\nFiles generated: ")
            files_generated = [globals.WRITE_OUTPUT_FILE,globals.WRITE_SQL_FILE]
            for i in files_generated:
                print(os.path.join(os.path.normpath(os.getcwd()), (i + self.truthname)))

    # ------------------------
    # write the common block
    def write_block_common(self):
        template_file = os.path.join(os.path.normpath(os.getcwd()), (globals.TEMPLATES_PATH + "common_head.txt"))
        content = ""
        try:
            if os.path.exists(self.job_filename):
                os.remove(self.job_filename)
        except OSError as err:
            print("Error while deleting old job file ", err)

        try:
            with open(template_file,'r',encoding='unicode_escape') as infile:
                content = infile.read()
                content = re.sub("<<sql_root>>" , globals.NAMINGCONV["sql_root"], content)
                content = re.sub("<<short_prefix>>", globals.NAMINGCONV["short_prefix"], content)
                content = re.sub("<<truthname>>", self.truthname, content)
                content = re.sub("<<JOB_ENVIRONS>>", globals.JOB_ENVIRONS, content)
        except FileNotFoundError as err:
            print("ERROR write_block_common: " , err)
        else:
            return(content)

    # ------------------------
    # write block bottom
    def write_block_bottom(self):
        content = "\n\t]\n}" #decide if this or a template
        with open(self.job_filename, 'ab+') as filehandle:
            filehandle.seek(-1, os.SEEK_END)
            filehandle.truncate()
        self.write_to_file(content)

    # ------------------------
    # write block parquet extract
    def write_block_parquet_extract(self,table_name,data_lake_location):
        template_file = os.path.join(os.path.normpath(os.getcwd()), (globals.TEMPLATES_PATH + "parquet_extract.txt"))
        content = ''
        current = '/cr/*"'

        try:
            with open(template_file, 'r', encoding='unicode_escape') as infile:
                content = infile.read()

                if 'CURRENT_DATE' in data_lake_location:
                    data_lake_location = re.sub("CURRENT_DATE", '\"${SQL_PARAM_CURRENT_DATE}', data_lake_location)
                    current = ''

                data_lake_location += current
                content = re.sub("<<data_lake_location>>", data_lake_location, content)
                content = re.sub("<<table_name>>", table_name, content)

        except FileNotFoundError as err:
            print(err, " while trying to create _job.json for parquet extract")

        else:
            return (content)

    # ------------------------
    # write to file
    def write_to_file(self,content):
        with open(self.job_filename, 'a') as outfile:
            outfile.write(content)
        outfile.close()

    # ------------------------
    # write schedule_file
    def create_schedule_file(self, filename, content):
        with open(filename, 'w') as outfile:
            outfile.write(content)
        outfile.close()
        self.truth_schedule = filename

    # ------------------------
    # write job file
    def write_job_file(self):
        file_path = self.get_file_path()
        filename = os.path.join(file_path, (self.truthname + "_job.json"))
        self.set_job_filename(filename)
        try:
            self.write_to_file(self.write_block_common())
            self.write_block_stage()
            self.write_block_bottom()
        except ValueError as err:
            print("Error: " , err)

    # ------------------------
    # write the stages of the json file
    def write_block_stage(self):
        count = 0
        for stage in self.truth_config:
            if stage[0] not in self._stage_type:
                print("Error write_block_stage: Invalid stage type: ", stage[0])
            else:
                if count != len(self.truth_config):
                    self._stage_type[stage[0]](stage[1:])

    # ------------------------
    # write parquet stage
    def stage_parquet_extract(self,args):
        self.write_to_file(self.write_block_parquet_extract(args[0], args[1]))

    # ------------------------
    # write sql transform stage
    def stage_sql_transform(self, args):
        for i in args:
            self.write_to_file(self.write_block_sql_transform(i))

    # ------------------------
    # write sql_transform_block
    def write_block_sql_transform(self,sql_file):
        template_file = os.path.join(os.path.normpath(os.getcwd()),(globals.TEMPLATES_PATH + "sql_transform.txt"))
        content = ""
        try:
            with open(template_file, 'r', encoding='unicode_escape') as infile:
                content = infile.read()
                content = re.sub("<<sql_name>>", sql_file, content)
                content = re.sub("<<name>>", sql_file, content)
        except FileNotFoundError as err:
                print(err, " while trying to create _job.json for sql_transfomr")

        else:
                return (content)

    # ------------------------
    # stage parquet load
    def stage_parquet_load(self, args):
        self.write_to_file(self.write_block_parquet_load(args))

    # ------------------------
    # write block parquet_load
    def write_block_parquet_load(self, args):
        template_file = os.path.join(os.path.normpath(os.getcwd()), (globals.TEMPLATES_PATH + "parquet_load.txt"))
        content = ""
        try:
            with open(template_file, 'r', encoding='unicode_escape') as infile:
                content = infile.read()
                content = re.sub("<<table_name>>", args[0], content)
                content = re.sub("<<data_lake_location>>", args[1], content)
        except FileNotFoundError as err:
            print(err, " while trying to create _job.json for parquet_load")

        else:
            return (content)

    # ------------------------
    # stage jdbc execute
    def stage_jdbc_execute(self, args):
        self.write_to_file(self.write_block_jdbc_execute(args))

    # ------------------------
    def write_block_jdbc_execute(self, args):
        if args[0] not in ("pre_staging","post_staging"):
            print("ERROR JDBCExecute :  value should be either pre/post_staging")
            quit(1)
        # part 1: generate the staging sql
        name = " ".join([args[0], "JDBC load for",globals.NAMINGCONV["truth_prefix"], self.truthname])
        staging_sql = "_".join([globals.NAMINGCONV["truth_prefix"], self.truthname, args[0]])
        template_file = os.path.join(os.path.normpath(os.getcwd()), (globals.TEMPLATES_PATH + "jdbc_execute.txt"))

        sql_dir = os.path.join(os.path.normpath(os.getcwd()),(globals.WRITE_SQL_FILE + "_".join([globals.NAMINGCONV["truth_prefix"], self.truthname])))
        if not os.path.exists(sql_dir):
            os.mkdir(sql_dir)
        sql_filename = os.path.join(sql_dir,(staging_sql + ".sql"))


        if args[0] == "pre_staging" :
            self.generate_pre_staging_sql(args[1],sql_filename)
        else :
            self.generate_post_staging_sql(sql_filename)


        # part 2: write the stage in job file
        content = ""
        try:
            with open(template_file, 'r', encoding='unicode_escape') as infile:
                content = infile.read()
                content = re.sub("<<name>>",name, content)
                content = re.sub("<<staging_sql>>",staging_sql,content)
        except FileNotFoundError as err:
            print(err, " while trying to create _job.json for parquet_load")

        else:
            return (content)


    # ------------------------
    # create prestaging file
    def generate_pre_staging_sql(self, view_scope ,sql_filename):
        # get the col_list !! as in downstream and not parquet !!
        col_list= ""
        try:
            columns_file = os.path.join(os.path.normpath(os.getcwd()),(globals.COLUMNS_PATH + self.truthname + "_columns.txt"))
            with open(columns_file,'r') as infile:
                col_list = infile.read()
        except FileNotFoundError as err:
            print("Column file not found for ",self.truthname)

        try:
            template_file = os.path.join(os.path.normpath(os.getcwd()),(globals.TEMPLATES_PATH + "prestaging_sql.txt"))
            with open(sql_filename, 'w') as outfile:
                with open(template_file, "r") as infile:
                    content = infile.read()
                    content = re.sub('<<tta_name>>', self.truthname, content)
                    content = re.sub('<<view_scope>>', view_scope, content)
                    content = re.sub('<<col_list>>', col_list, content)
                    outfile.write(content)

        except FileNotFoundError as error:
            print(error)


    def generate_post_staging_sql(self,sql_filename):
        try:
            template_file=os.path.join(os.path.normpath(os.getcwd()), (globals.TEMPLATES_PATH + "poststaging_sql.txt"))
            with open(sql_filename, 'w') as outfile:
                with open(template_file, "r") as infile:
                    content = infile.read()
                    content = re.sub('<<tta_name>>', self.truthname, content)
                    outfile.write(content)

        except FileNotFoundError as error:
            print(error)

    # ------------------------
    # stage jdbc load
    def stage_jdbc_load(self, args):
        self.write_to_file(self.write_block_jdbc_load(args))

    def write_block_jdbc_load(selfself,args):
        template_file = os.path.join(os.path.normpath(os.getcwd()), (globals.TEMPLATES_PATH + "jdbc_load.txt"))
        content = ""
        try:
            with open(template_file, 'r', encoding='unicode_escape') as infile:
                content = infile.read()
                content = re.sub("<<input_view>>", args[0], content)
                content = re.sub("<<staging_table>>", args[1]+"_staging", content)
        except FileNotFoundError as err:
            print(err, " while trying to create _job.json for parquet_load")

        else:
            return (content)

    # ------------------------
    # write schedule file
    def write_schedule_file(self,line = []):
        template_file = os.path.join(os.path.normpath(os.getcwd()), (globals.TEMPLATES_PATH + "schedule_block.txt"))
        line = self.truth_config[0]
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

    # ------------------------
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

    # ------------------------
    # schedule_job_now(vm_size)
    def schedule_job_now(self,size):
        if size not in globals.SIZE_OPTIONS:
            print("ERROR3: invalid job size ")
            return False
        else:
            content = self.replace_schedule_file(size)
            file_path = self.get_file_path()
            filename = os.path.join(file_path ,(self.truthname +"_schedule.json"))
            self.create_schedule_file(filename,content)

    # ------------------------
    def get_file_path(self):
        if os.path.exists(os.path.join(os.path.normpath(os.getcwd()), (globals.WRITE_OUTPUT_FILE + self.truthname))):
            return ((os.path.join(os.path.normpath(os.getcwd()), (globals.WRITE_OUTPUT_FILE + self.truthname))))
        else:
            return (os.mkdir(os.path.join(os.path.normpath(os.getcwd()), (globals.WRITE_OUTPUT_FILE + self.truthname))))

    # ------------------------
    # schedule job for future
    def schedule_job_future(self,schedule):
        date,time,interval,size = schedule
        if date == '' or time == '' or interval== '' :
            print('invalid config file: cannot have an null start date & time or interval for a future job')
            return False
        else:
            flag = self.check_valid_future_date_time(date,time)
            if flag:
                flag = self.check_valid_interval(interval)
                # write to schedule fine now
                day,month,year = date.split('/')
                start_date = '/'.join([year,month,day]) # framework expect yyy/mm/day hh:mm:ss format
                content = self.replace_schedule_file(size,' '.join([start_date,time]),interval)
                file_path = self.get_file_path()
                filename = os.path.join(file_path, (self.truthname + "_schedule.json"))
                self.create_schedule_file(filename, content)

            else:
                return False

    # ------------------------
    # check valid date
    def check_valid_future_date_time(self,date,time):
        # date format : dd/mm/yyyy
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

    # ------------------------
    # check valid time
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
            if str(schedule_time) <  min_delta:
                print("ERROR: Schedule time to be atleast an hour from now")
                return False
        else:
            return True

    # ------------------------
    # check valid interval
    def check_valid_interval(self,interval):
        if not (type(interval) == 'int') or int(interval) < 60:
            # repeat cycle to be min 60 mins due to batch spin and build time
            return False
        else:
            return True


