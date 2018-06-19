
from datetime import datetime, date, time, timedelta
import math
import copy
import random
from scipy import stats
import numpy as np
from collections import OrderedDict
import statsmodels.api as sm
import pandas as pd

from .Time_Series import *
from .Bout import *
from .Channel import *
from .channel_inference import *
from .hdf5 import *

def get_calibrate(hdf5_group):
    """
    This hdf5 file object contains a calirbate cache, load it and return the results
    """

    return dictionary_from_attributes(hdf5_group)


def set_calibrate(cache, hdf5_group):
    """
    Create a group for the cached results of calibrate() and write the cache to it
    """

    dictionary_to_attributes(cache, hdf5_group)


def is_calibrated(channel):
    """
    Return True if the channel appears to have been autocalibrated already.
    """

    return hasattr(channel, "calibrated") and channel.calibrated == True


def nearest_sphere_surface(x_input, y_input, z_input):
    """Given the 3D co-ordinates of a point, return the 3D co-ordinates of the point on the surface of a unit sphere. """

    vm = math.sqrt(sum([x_input**2, y_input**2, z_input**2]))
    return (x_input/vm, y_input/vm, z_input/vm)


def dataframe_regression(df, cal_mode, do_or_undo="do"):
    """Given a dataframe(df), perform liner regression on the columns required, according to the cal_mode variable.
       "do_or_undo" variable determines the direction of the regression.

       The required columns in the dataframe:
       df.X_matched, df.Y_matched, df.Z_matched (the values of x,y,z matched to closest sphere surface point)
       df.X_orig, df.Y_orig, df.Z_orig (the original values of x,y,z that are preserved)
       df.X, df.Y, df.Z (the values of x,y,z that update after the regression)
       df.intercept (column of ones to act as intercept)
       df.T_dev (column of deviation in temperature from the optimal - ONLY USED IF cal_mode = "offset_scale_temp" or "offset_temp")
    """

    # perform linear regression to optimise to matched column
    if do_or_undo == "do":

        # if temperature is used in calibration
        if "temp" in cal_mode:
            x_results = sm.OLS(df.X_matched, df[["X", "intercept", "T_dev"]]).fit()
            y_results = sm.OLS(df.Y_matched, df[["Y", "intercept", "T_dev"]]).fit()
            z_results = sm.OLS(df.Z_matched, df[["Z", "intercept", "T_dev"]]).fit()
        # if temperature NOT used in calibration
        else:
            x_results = sm.OLS(df.X_matched, df[["X", "intercept"]]).fit()
            y_results = sm.OLS(df.Y_matched, df[["Y", "intercept"]]).fit()
            z_results = sm.OLS(df.Z_matched, df[["Z", "intercept"]]).fit()


    # perform linear regression to optimise the transformed x,y,z data back to the original x,y,z data
    elif do_or_undo == "undo":

        # if temperature was used in calibration
        if "temp" in cal_mode:
            x_results = sm.OLS(df["X"], df[["X_orig", "intercept", "T_dev"]]).fit()
            y_results = sm.OLS(df["Y"], df[["Y_orig", "intercept", "T_dev"]]).fit()
            z_results = sm.OLS(df["Z"], df[["Z_orig", "intercept", "T_dev"]]).fit()
        # if temperature was NOT used in calibration
        else:
            x_results = sm.OLS(df["X"], df[["X_orig", "intercept"]]).fit()
            y_results = sm.OLS(df["Y"], df[["Y_orig", "intercept"]]).fit()
            z_results = sm.OLS(df["Z"], df[["Z_orig", "intercept"]]).fit()

    return x_results, y_results, z_results


def dataframe_transformation(df, x_results, y_results, z_results, cal_mode):
    """Given the output from the function dataframe_regression() transform the columns df.X, df.Y, df.Z in a given dataframe, depending on the value of the cal_mode variable.

    results.params() gives the calibration parameters thus:
    x_results.params() = [x_scale, x_offset, x_temp_offset]   (last item only applies if temperature is used)"""

    if cal_mode == "offset":
        # Transform the input points using ONLY the offset co-efficient
        # e.g. X(t) = X(t) + x_offset
        df.X = df.X + x_results.params[1]
        df.Y = df.Y + y_results.params[1]
        df.Z = df.Z + z_results.params[1]

    elif cal_mode == "offset_scale":
        # Transform the input points using the regression co-efficients for offset and scale
        # e.g. X(t) = (X(t)* x_scale) + x_offset
        df.X = (df.X * x_results.params[0]) + x_results.params[1]
        df.Y = (df.Y * y_results.params[0]) + y_results.params[1]
        df.Z = (df.Z * z_results.params[0]) + z_results.params[1]

    elif cal_mode == "offset_temp":
        # Transform the input points using the regression co-efficients for offset and the temperature-scaled offset
        # e.g. X(t) = (X(t) + x_offset + (T_dev(t)*temp_offset)
        df.X = df.X + x_results.params[1] + (df.T_dev * x_results.params[2])
        df.Y = df.Y + y_results.params[1] + (df.T_dev * y_results.params[2])
        df.Z = df.Z + z_results.params[1] + (df.T_dev * z_results.params[2])

    elif cal_mode == "offset_scale_temp":
        # Transform the input points using the regression co-efficients for offset and scale and the temperature-scaled offset
        # e.g. X(t) = (X(t)* x_scale) + x_offset + (T_dev(t)*temp_offset)
        df.X = (df.X * x_results.params[0]) + x_results.params[1] + (df.T_dev * x_results.params[2])
        df.Y = (df.Y * y_results.params[0]) + y_results.params[1] + (df.T_dev * y_results.params[2])
        df.Z = (df.Z * z_results.params[0]) + z_results.params[1] + (df.T_dev * z_results.params[2])

    return df


def find_calibration_parameters(x_input, y_input, z_input, temperature, cal_mode, optimal_t = 25, num_iterations=1000):
    """Find the offset and scaling factors for each 3D axis. Assumes the input vectors are only still points."""

    # Need to keep a copy of the original input
    x_input_copy = x_input[::]
    y_input_copy = y_input[::]
    z_input_copy = z_input[::]

    # Need 3 blank arrays to populate
    x_matched = np.empty(len(x_input))
    y_matched = np.empty(len(y_input))
    z_matched = np.empty(len(z_input))

    df = pd.DataFrame()
    df["X_orig"] = x_input_copy
    df["Y_orig"] = y_input_copy
    df["Z_orig"] = z_input_copy
    df["X"] = x_input
    df["Y"] = y_input
    df["Z"] = z_input

    df["intercept"] = 1

    if "temp" in cal_mode:
        # create a column of T - optimal_T (mean temperature for each still bout minus the optimal temperature) i.e. the deviation in T from the optimal
        df["T_dev"] = temperature.data - optimal_t


    for i in range(num_iterations):

        for i,a,b,c in zip(range(len(x_input)),x_input, y_input, z_input):

            # For each point, find its nearest point on the surface of a sphere
            closest = nearest_sphere_surface(a,b,c)

            # Put the result in the X,Y,Z arrays
            x_matched[i] = closest[0]
            y_matched[i] = closest[1]
            z_matched[i] = closest[2]

        # Add matched arrays to DataFrame
        df["X_matched"] = x_matched
        df["Y_matched"] = y_matched
        df["Z_matched"] = z_matched

  # Now that each X input is matched up against a "perfect" X on a sphere, do linear regression:
        x_results, y_results, z_results = dataframe_regression(df, cal_mode, do_or_undo="do")

        # results.params() gives the calibration parameters thus:
        # x_results.params() = [x_scale, x_offset, x_temp_offset]   (last item only applies if temperature is used)

        df = dataframe_transformation(df, x_results, y_results, z_results, cal_mode)

    # Regress the backup copy of the original input against the transformed version,
    # to calculate offset, scale and temperature offset scalar (if temperature used)
    x_results_final, y_results_final, z_results_final = dataframe_regression(df, cal_mode, do_or_undo="undo")

    calibration_parameters = {"x_offset": x_results_final.params[1],
                              "x_scale": x_results_final.params[0],
                              "y_offset": y_results_final.params[1],
                              "y_scale": y_results_final.params[0],
                              "z_offset": z_results_final.params[1],
                              "z_scale": z_results_final.params[0]
                              }

    if "temp" in cal_mode:
        calibration_parameters["x_temp_offset"] = x_results_final.params[2]
        calibration_parameters["y_temp_offset"] = y_results_final.params[2]
        calibration_parameters["z_temp_offset"] = z_results_final.params[2]

    return calibration_parameters


def calibrate_slave(x, y, z, budget=1000, noise_cutoff_mg=13):
    """
    Slave to calibrate()
    """

    stillbouts_ts, calibration_diagnostics = calibrate_stepone(x, y, z, budget=1000, noise_cutoff_mg=13)

    calibration_diagnostics = calibrate_steptwo(stillbouts_ts, calibration_diagnostics)

    return calibration_diagnostics


def calibrate_stepone(x, y, z, temperature=None, battery=None, budget=1000, noise_cutoff_mg=13):
    # All diagnostics and results will be saved to this dictionary
    # calibrate() returns this dictionary, and passes it to hdf5.dictionary_to_attributes() for caching
    stillbouts_ts = Time_Series("")
    calibration_diagnostics = OrderedDict()

    # Saving passed parameters for later reference
    calibration_diagnostics["budget"] = budget
    calibration_diagnostics["noise_cutoff_mg"] = noise_cutoff_mg

    vm = infer_vector_magnitude(x,y,z)

    # Get a list of bouts where standard deviation in each axis is below given threshold ("still")
    still_bouts = infer_still_bouts_triaxial(x,y,z, noise_cutoff_mg=noise_cutoff_mg, minimum_length=timedelta(minutes=1))
    num_still_bouts = len(still_bouts)
    num_still_seconds = total_time(still_bouts).total_seconds()

    # Summarise VM in 10s intervals
    vm_windows = vm.piecewise_statistics(timedelta(seconds=10), [("generic", ["mean"])], time_period=vm.timeframe)[0]

    # Get a list where VM was between 0.5 and 1.5g ("reasonable")
    reasonable_bouts = vm_windows.bouts(0.5, 1.5)
    num_reasonable_bouts = len(reasonable_bouts)
    num_reasonable_seconds = total_time(reasonable_bouts).total_seconds()

    # We only want still bouts where the VM level was within 0.5g of 1g
    # Therefore intersect "still" time with "reasonable" time
    still_bouts = bout_list_intersection(reasonable_bouts, still_bouts)

    # And we only want bouts where it was still and reasonable for 10s or longer
    still_bouts = limit_to_lengths(still_bouts, min_length = timedelta(seconds=10))
    num_final_bouts = len(still_bouts)
    num_final_seconds = total_time(still_bouts).total_seconds()

    # Get the average X,Y,Z for each still bout (inside which, by definition, XYZ should not change)
    still_x, std_x, num_samples = x.build_statistics_channels(still_bouts, [("generic", ["mean", "std", "n"])])
    still_y, std_y = y.build_statistics_channels(still_bouts, [("generic", ["mean", "std"])])
    still_z, std_z = z.build_statistics_channels(still_bouts, [("generic", ["mean", "std"])])

    channels = [num_samples, still_x, std_x, still_y, std_y, still_z, std_z]
    # Add the statistics channels to the empty still bouts Time Series
    stillbouts_ts.add_channels(channels)

    # if temperature data is required build the statistics channels and add to the still bouts Time Series
    if temperature is not None:
        still_temperature, std_temperature = temperature.build_statistics_channels(still_bouts, [("generic", ["mean", "std"])])
        calibration_diagnostics["mean_temperature"] = np.mean(temperature.data)
        calibration_diagnostics["min_temperature"] = np.min(temperature.data)
        calibration_diagnostics["max_temperature"] = np.max(temperature.data)
        calibration_diagnostics["std_temperature"] = np.std(temperature.data)
        temp_channels = [still_temperature, std_temperature]
        stillbouts_ts.add_channels(temp_channels)

    # if battery data is required build the statistics channel and add to the still bouts Time Series
    if battery is not None:
        still_battery = battery.build_statistics_channels(still_bouts, [("generic", ["mean"])])[0]
        calibration_diagnostics["min_battery"] = np.min(temperature.data)
        calibration_diagnostics["max_battery"] = np.max(battery.data)
        stillbouts_ts.add_channel(still_battery)

    # Still bouts information
    calibration_diagnostics["num_final_bouts"] = num_final_bouts
    calibration_diagnostics["num_final_seconds"] = num_final_seconds
    calibration_diagnostics["num_still_bouts"] = num_still_bouts
    calibration_diagnostics["num_still_seconds"] = num_still_seconds
    calibration_diagnostics["num_reasonable_bouts"] = num_reasonable_bouts
    calibration_diagnostics["num_reasonable_seconds"] = num_reasonable_seconds

    return (stillbouts_ts, calibration_diagnostics)


def calibrate_steptwo(stillbouts_ts, calibration_diagnostics, cal_method="best_fit"):

    still_x = stillbouts_ts["X_mean"]
    still_y = stillbouts_ts["Y_mean"]
    still_z = stillbouts_ts["Z_mean"]
    num_samples = stillbouts_ts["X_n"]

    # if best fit of calibration is required
    if cal_method == "best_fit":
        # Ascertain if temperature data is present:
        try:
            still_temperature = stillbouts_ts["Temperature_mean"]
        except:
            still_temperature = None
    # if temperature data is not to be used for calibration
    elif cal_method == "force_no_temp":
        still_temperature = None

    # Get the octant positions of the points to calibrate on
    occupancy = octant_occupancy(still_x.data, still_y.data, still_z.data)

    # Are they fairly distributed?
    comparisons = {"x<0":[0,1,2,3], "x>0":[4,5,6,7], "y<0":[0,1,4,5], "y>0":[2,3,6,7], "z<0":[0,2,4,6], "z>0":[1,3,5,7]}
    for axis in ["x", "y", "z"]:
        mt = sum(occupancy[comparisons[axis + ">0"]])
        lt = sum(occupancy[comparisons[axis + "<0"]])
        calibration_diagnostics[axis + "_inequality"] = abs(mt-lt)/sum(occupancy)

    # Calculate the initial error without doing any calibration
    # i.e. set the parameters to an 'ideal'
    ideal_parameters = {"x_offset": 0,
                        "x_scale": 1,
                        "x_temp_offset": 0,
                        "y_offset": 0,
                        "y_scale": 1,
                        "y_temp_offset": 0,
                        "z_offset": 0,
                        "z_scale": 1,
                        "z_temp_offset": 0}

    start_error = evaluate_solution(still_x, still_y, still_z, num_samples, ideal_parameters)

    # Set the calibration method:
    #    offset = use offset factors only
    #    offset_scale = use offset and scale factors
    #    offset_temp = use offset and temperature offset
    #    offset_scale_temp = use offset, scale and temperature offset

    # If we have less than 500 points to calibrate with, or if more than 2 octants are empty we will not use scale:
    use_scale = True
    if len(still_x.data) < 500 or sum(occupancy == 0) > 2:
        use_scale = False

    # Assign calibration method according to parameters 'use_scale' and 'still_temperature'
    if not use_scale and still_temperature is None:
        cal_mode = "offset"
        calibration_diagnostics["calibration_method"] = "offset only"

    elif not use_scale and still_temperature is not None:
        cal_mode = "offset_temp"
        calibration_diagnostics["calibration_method"] = "offset only with temperature"

    elif use_scale and still_temperature is None:
        cal_mode = "offset_scale"
        calibration_diagnostics["calibration_method"] = "offset and scale"

    elif use_scale and still_temperature is not None:
        cal_mode = "offset_scale_temp"
        calibration_diagnostics["calibration_method"] = "offset and scale with temperature"

    # Search for the correct way to calibrate the data
    calibration_parameters = find_calibration_parameters(still_x.data, still_y.data, still_z.data, still_temperature, cal_mode)

    # update the calibration_diagnostics dictionary with the calibration parameters
    calibration_diagnostics.update(calibration_parameters)

    for i,occ in enumerate(occupancy):
        calibration_diagnostics["octant_"+str(i)] = occ

    # Calculate the final error after calibration
    end_error = evaluate_solution(still_x, still_y, still_z, num_samples, calibration_parameters, still_temperature)

    calibration_diagnostics["start_error"] = start_error
    calibration_diagnostics["end_error"] = end_error

    return calibration_diagnostics


def calibrate(x, y, z, temperature=None, budget=1000, noise_cutoff_mg=13, hdf5_file=None):
    """ Use still bouts in the given triaxial data to calibrate it and return the calibrated channels """

    args = {"x":x, "y":y, "z":z, "budget":budget, "noise_cutoff_mg":noise_cutoff_mg}
    params = ["budget", "noise_cutoff_mg"]
    calibration_diagnostics = do_if_not_cached("calibrate", calibrate_slave, args, params, get_calibrate, set_calibrate, hdf5_file)

    # Regardless of how we get the results, extract the offset and scales
    calibration_parameters = [calibration_diagnostics[var] for var in ["x_offset", "x_scale", "y_offset", "y_scale", "z_offset", "z_scale"]]

    if temperature is not None:
        calibration_parameters = [calibration_diagnostics[var] for var in ["x_temp_offset", "y_temp_offset", "z_temp_offset"]]

    # Apply the best calibration factors to the data
    do_calibration(x, y, z, temperature, calibration_parameters)

    return (x, y, z, calibration_diagnostics)


def do_calibration(x,y,z,t,cp):
    """
    Performs calibration on given channel using a given dictionary of parameters (cp)
     """
    # if temperature is used for calibration:
    if t is not None:
        x.data = cp["x_offset"] + (t.data * cp["x_temp_offset"]) + (x.data / cp["x_scale"])
        y.data = cp["y_offset"] + (t.data * cp["y_temp_offset"]) + (y.data / cp["y_scale"])
        z.data = cp["z_offset"] + (t.data * cp["z_temp_offset"]) + (z.data / cp["z_scale"])

        x.temp_offset = cp["x_temp_offset"]
        y.temp_offset = cp["y_temp_offset"]
        z.temp_offset = cp["z_temp_offset"]

    # if temperature is not used for calibration:
    else:
        x.data = cp["x_offset"] + (x.data / cp["x_scale"])
        y.data = cp["y_offset"] + (y.data / cp["y_scale"])
        z.data = cp["z_offset"] + (z.data / cp["z_scale"])

    x.offset = cp["x_offset"]
    x.scale = cp["x_scale"]
    x.calibrated = True

    y.offset = cp["y_offset"]
    y.scale = cp["y_scale"]
    y.calibrated = True

    z.offset = cp["z_offset"]
    z.scale = cp["z_scale"]
    z.calibrated = True


def undo_calibration(x,y,z,t,cp):
    """
    Reverses calibration on given channel using a given dictionary of parameters (cp)
    """

    if t is not None:
        x.data = -cp["x_offset"] - (t.data * cp["x_temp_offset"]) + (x.data / cp["x_scale"])
        y.data = -cp["y_offset"] - (t.data * cp["y_temp_offset"]) + (y.data / cp["y_scale"])
        z.data = -cp["z_offset"] - (t.data * cp["z_temp_offset"]) + (z.data / cp["z_scale"])

    else:
        x.data = -cp["x_offset"] + (x.data / cp["x_scale"])
        y.data = -cp["y_offset"] + (y.data / cp["y_scale"])
        z.data = -cp["z_offset"] + (z.data / cp["z_scale"])

    x.calibrated = False
    y.calibrated = False
    z.calibrated = False


def undo_calibration_using_diagnostics(x,y,z,cd):
    """
    Convenience function that pulls the offset and scale values out of a regular calibration diagnostics dictionary.
    """
    undo_calibration(x, y, z, [cd["x_offset"],cd["x_scale"],cd["y_offset"],cd["y_scale"],cd["z_offset"],cd["z_scale"]] )


def evaluate_solution(still_x, still_y, still_z, still_n, calibration_parameters, still_temperature=None):
    """ Calculates the RMSE of the input XYZ signal if calibrated according to input calibration parameters"""

    # if temperature not involved in calibration then set temperature offset scalar values to zero
    if still_temperature is None:
        calibration_parameters["x_temp_offset"] = 0
        calibration_parameters["y_temp_offset"] = 0
        calibration_parameters["z_temp_offset"] = 0

    # Temporarily adjust the channels of still data, which has collapsed x,y,z values
    do_calibration(still_x, still_y, still_z, still_temperature, calibration_parameters)

    # Get the VM of the calibrated channel
    vm = infer_vector_magnitude(still_x, still_y, still_z)

    # se = sum error
    se = 0.0

    for vm_val,n in zip(vm.data, still_n.data):
        se += (abs(1.0 - vm_val)**2)*n

    rmse = math.sqrt(se / len(vm.data))

    # Undo the temporary calibration
    undo_calibration(still_x, still_y, still_z, still_temperature, calibration_parameters)

    return rmse


def octant_occupancy(x, y, z):
    """ Counts number of samples lying in each octal region around the origin """

    octants = np.zeros(8, dtype="int")

    for a,b,c in zip(x,y,z):

        if a < 0 and b < 0 and c < 0:
            octants[0] += 1
        elif a < 0 and b < 0 and c > 0:
            octants[1] += 1
        elif a < 0 and b > 0 and c < 0:
            octants[2] += 1
        elif a < 0 and b > 0 and c > 0:
            octants[3] += 1
        elif a > 0 and b < 0 and c < 0:
            octants[4] += 1
        elif a > 0 and b < 0 and c > 0:
            octants[5] += 1
        elif a > 0 and b > 0 and c < 0:
            octants[6] += 1
        elif a > 0 and b > 0 and c > 0:
            octants[7] += 1
        else:
            # Possible because of edge cases, shouldn't come up in calibration
            pass

    return octants
