import collections
import math
import numpy as np
import sys
from datetime import datetime
import json
import traceback
import pandas as pd

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


def get_feedback(filename):

    file = open(filename, "r")
    feedback = json.loads(file.read())
    file.close()
    return feedback


def write_feedback(feedback, filename):

    file = open(filename, "w")
    file.write(json.dumps(feedback))
    file.flush()
    file.close()


def load_job_details(job_file):

    data = np.loadtxt(job_file, delimiter=',', dtype='S', comments="#!<>", skiprows=0).astype("U")

    master_dictionary = collections.OrderedDict()

    for row in data[1:]:
        master_dictionary[row[0]] = {}
        for index,col in enumerate(row):
            master_dictionary[row[0]][data[0,index]] = col

    return master_dictionary


def batch_process_list(analysis_function, job_file, job_num=1, num_jobs=1, live_feedback=False):
    """
    Generates a dictionary of job details from a job file with just one column, ready to pass to batch_process_steptwo      below.
        """
    batch_start_time = datetime.now()

    # Load the job list document
    s = pd.read_csv(job_file, squeeze=True)
    job_details = s.to_dict()

    batch_process_steptwo(analysis_function, batch_start_time, job_file, job_details, job_num, num_jobs, live_feedback)


def batch_process(analysis_function, job_file, job_num=1, num_jobs=1, live_feedback=False):
    """
    Generates a dictionary of job details from a job file with more than one column, ready to pass to batch_process_steptwo below.
    """

    batch_start_time = datetime.now()

    # Load the document listing all the files to be processed
    job_details = load_job_details(job_file)

    batch_process_steptwo(analysis_function, batch_start_time, job_file, job_details, job_num, num_jobs, live_feedback)


def batch_process_steptwo(analysis_function, batch_start_time, job_file, job_details, job_num, num_jobs, live_feedback):
    """
    The bulk of the batch process of an analysis function.  Performed once a dictionary of job details has been generated.
    """

    # Using job_num and num_jobs, calculate which files this process should handle
    job_section = job_indices(job_num, num_jobs, len(job_details))
    my_jobs = list(job_details.keys())[job_section[0]:job_section[1]]

    task_name = analysis_function.__name__

    error_log = False
    output_log = open(task_name + "_output_{}.csv".format(job_num), "w")

    if live_feedback:
        # Create a JSON file to store progress information in
        feedback_filename = job_file + "_{}_status.json".format(job_num)
        write_feedback({"job":job_num, "num_jobs":len(job_details), "progress":1, "complete":0}, feedback_filename)

    for n, job in enumerate(my_jobs):

        output_log.write("\nJob {}/{}: {}\n".format(n+1, len(my_jobs), job))
        job_start_time = datetime.now()
        output_log.write("\nJob start time: " + str(job_start_time))
        output_log.flush()

        try:
            if live_feedback:
                analysis_function( job_details[job], feedback_filename )
            else:
                analysis_function( job_details[job] )

        except:

            tb = traceback.format_exc()

            # Create the error file only if an error has occurred
            if error_log is False:
                error_log = open(task_name + "_error_{}.csv".format(job_num), "w")

            print("Exception:" + str(sys.exc_info()))
            print(tb)

            error_log.write( str(job_details[job]) + "\n" )
            error_log.write("Exception:" + str(sys.exc_info()) + "\n")
            error_log.write(tb + "\n\n")
            error_log.flush()

        job_end_time = datetime.now()
        job_duration = job_end_time - job_start_time
        output_log.write("\nJob run time: " + str(job_duration))

        batch_duration = job_end_time - batch_start_time
        batch_remaining = (len(my_jobs)-n)*job_duration
        output_log.write("\nBatch run time: " + str(batch_duration))
        output_log.write("\nTime remaining: " + str(batch_remaining))
        output_log.write("\nPredicted completion time:" + str((batch_remaining + datetime.now())) + "\n")
        output_log.flush()

        if live_feedback:
            feedback = get_feedback(feedback_filename)
            feedback["progress"] += 1
            write_feedback(feedback, feedback_filename)

    batch_end_time = datetime.now()
    batch_duration = batch_end_time - batch_start_time
    output_log.write("\nBatch run time: " + str(batch_duration))
    output_log.flush()
    output_log.close()

    if live_feedback:
        feedback = get_feedback(feedback_filename)
        feedback["complete"] = 1
        write_feedback(feedback, feedback_filename)

    # If everything went smoothly, error_log is False because it was never a file object
    if error_log is not False:
        error_log.close()
