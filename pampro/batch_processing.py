import collections
import math
import numpy as np
import sys, os
from datetime import datetime
import json
import traceback
import pandas as pd
import glob

def job_indices(n, num_jobs, job_list_size):

    n = n-1

    job_size = math.floor(job_list_size/num_jobs)
    remaining = job_list_size - (num_jobs*job_size)

    start_index = 0
    for i in range(num_jobs):

        end_index = min( job_list_size, start_index + job_size )

        if remaining > 0:
            end_index += 1
            remaining -= 1

        if i == n:
            return (start_index,end_index)
        start_index = end_index


def batch_process(analysis_function, jobs_spec, status_folder=None, task=None, job_num=1, num_jobs=1):
    """
    Generates a dictionary of job details from a job file with more than one column, ready to pass to batch_process_steptwo below.
    """

    batch_start_time = datetime.now()

    if type(jobs_spec) is str:
        
        # Load the document listing all the files to be processed
        # read in the file
        df = pd.read_csv(jobs_spec)
        
    else:
    
        df = jobs_spec 


    # Using job_num and num_jobs, calculate which files this process should handle
    job_section = job_indices(job_num, num_jobs, len(df))
    my_jobs = df[job_section[0]:job_section[1]]

    if task is None:
        task = analysis_function.__name__

    error_log = False
    output_log = open("_logs" + os.sep + task + "_output_{}.csv".format(job_num), "w")

    for n, job in my_jobs.iterrows():

        successful = False
        
        output_log.write("\nJob {}/{}\n".format(n+1, len(my_jobs)))
        for index in job.index:
            output_log.write("{}: {}".format(index, job[index]))
        
        job_start_time = datetime.now()
        output_log.write("\nJob start time: " + str(job_start_time))
        output_log.flush()

        try:
            output_dict = analysis_function(job)
            
            successful = True
            
            
        except:

            tb = traceback.format_exc()

            # Create the error file only if an error has occurred
            if error_log is False:
                error_log = open("_logs" + os.sep + task + "_error_{}.csv".format(job_num), "w")

            print("Exception:" + str(sys.exc_info()))
            print(tb)

            error_log.write( str(job) + "\n" )
            error_log.write("Exception:" + str(sys.exc_info()) + "\n")
            error_log.write(tb + "\n\n")
            error_log.flush()

        data_filename = job["data_filename"]   
        status_to_append = {task + "_executed": True,
                            task + "_successful": successful}
                            
        if output_dict is not None:
            for k,v in output_dict.items():
                status_to_append[k] = v
         
        if status_folder:
            update_status_from_filename(data_filename, status_folder, status_to_append)
        
        job_end_time = datetime.now()
        job_duration = job_end_time - job_start_time
        output_log.write("\nJob run time: " + str(job_duration))

        batch_duration = job_end_time - batch_start_time
        batch_remaining = (len(my_jobs)-n)*job_duration
        output_log.write("\nBatch run time: " + str(batch_duration))
        output_log.write("\nTime remaining: " + str(batch_remaining))
        output_log.write("\nPredicted completion time:" + str((batch_remaining + datetime.now())) + "\n")
        output_log.flush()

    batch_end_time = datetime.now()
    batch_duration = batch_end_time - batch_start_time
    output_log.write("\nBatch run time: " + str(batch_duration))
    output_log.flush()
    output_log.close()

    # If everything went smoothly, error_log is False because it was never a file object
    if error_log is not False:
        error_log.close()
        


def update_status_from_filename(filename, status_folder, status_to_append):

    status = get_status_from_filename(filename, status_folder)

    for k,v in status_to_append.items():
        status[k] = v
    
    set_status(filename, status_folder, status)
    

def get_status_from_filename(filename, status_folder):
    head, tail = os.path.split(filename)
    name = tail.split(".")[0] + "_status.json"
    
    status_filename = status_folder + os.sep + name
    
    if os.path.isfile(status_filename):
        with open(status_filename, "r") as sf:
            status = json.loads(sf.read())
    
    return status
    
    
def get_status_from_status_file(status_filename, status_folder):

    if os.path.isfile(status_filename):
        with open(status_filename, "r") as sf:
            status = json.loads(sf.read())
    
    return status


def set_status(filename, status_folder, status=None):
    
    if not status:
        status = {} 
    
    head, tail = os.path.split(filename)
    name = tail.split(".")[0] + "_status.json"

    status_filename = status_folder + os.sep + name
    
    if len(status) == 0:
        status["data_filename"] = filename
    
    with open(status_filename, "w") as sf:        
        sf.write(json.dumps(status))
    


def list_all_status_files(status_folder):
    status_files = glob.glob(status_folder + os.sep + "*_status.json")
    
    return status_files


def list_all_raw_files(data_folder):
    data_files = glob.glob(data_folder + os.sep + "*.*")
    
    return data_files


def is_task_executed(status, task):
    
    task_executed = task + "_executed"
    
    if task_executed in status:
        return status[task_executed]
    else:
        return False
        
    
def is_task_successful(status, task):    
    
    task_successful = task + "_successful"
    
    if task_successful in status:
        return status[task_successful]
    else:
        return False
        

def ready_for_task(status_folder, task, prerequisites=[]):

    status_files = list_all_status_files(status_folder)
    
    ready = []
    for status_file in status_files:
        status = get_status_from_status_file(status_file, status_folder)
        if prerequisite_satisfied(status, prerequisites):
            if not is_task_successful(status, task):
                ready.append(status["data_filename"])

    df = pd.DataFrame({"data_filename": ready})
    
    return df
    
    
def ensure_status_files(data_folder, status_folder):

    filenames_lacking_status = files_without_status(data_folder, status_folder)
    
    for filename in filenames_lacking_status:
        set_status(filename, status_folder)
        

def files_without_status(data_folder, status_folder):
    
    status_files = list_all_status_files(status_folder)
    
    data_files = list_all_raw_files(data_folder)
    print("num status", len(status_files))
    print("num data", len(data_files))
    
    status_files_map = {}
    
    status_files_set = set()
    for f in status_files:
        head, tail = os.path.split(f)
        name = tail.split(".")[0]
        name = name.replace("_status", "")
        status_files_set.add(name)
        status_files_map[name] = f
        
    data_files_map = {}
    data_files_set = set()
    for f in data_files:
        head, tail = os.path.split(f)
        name = tail.split(".")[0]
        data_files_set.add(name)
        data_files_map[name] = f
     
    lacking_status = data_files_set - status_files_set
    print("num lacking status", len(lacking_status))
    
    filenames_lacking_status = [data_files_map[name] for name in lacking_status]
    print(filenames_lacking_status)
    
    return filenames_lacking_status
    
    
def prerequisite_satisfied(status, prerequisites):

    for prerequisite in prerequisites:
        if not is_task_successful(status, prerequisite):
            return False
    
    return True
    
