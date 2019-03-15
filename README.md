## pampro - physical activity monitor processing

### Introduction

pampro is a software project for the systematic analysis of physical activity data collected in epidemiological studies. The ultimate goal is to provide a turn-key solution for physical activity data analysis, replicating published methodologies in a monitor-agnostic framework.


### New in version v0.5.0 

* Import additional time series data (light, battery level and temperature data, depending on monitor type) from these common monitor file types:

	* Axivity binary (.cwa)
	* GeneActiv (.bin)
	* Actigraph GT3X+ csv files(.csv)
	
* Produce a "database" of calibration parameters using autocalibration of multiple files produced by the same monitor.

* Use the calibration "database" to calibrate files.

* A new 'diagnostics' module for the detection and resolution of timestamping anomalies within raw Axivity binary (.cwa) or GeneActiv (.bin) files.


### Features

* Import channels of time series data from a variety of common monitors:
	
	* ActiHeart (.txt)
	* Axivity binary (.cwa)
	* GeneActiv (.bin)
	* Actigraph (.dat)
	* activPAL & activPAL micro binary (.datx)
	* Any timestamped data (.csv)

* Output piecewise summary statistics of any data channel, over any size time window:
	
	* Time spent in any cutpoint.
	* Sum, mean, percentiles, min, max.

* Visualise the time series data.

* Extract bouts of activity in any cutpoint.

* Various triaxial acceleration methodologies:
	
	* Nonwear detection.
	* Autocalibration.

* Conversion of raw waveform files to HDF5 files (see below).

### The use of [HDF5](https://www.hdfgroup.org/HDF5/)

The main feature of the preceeding version (v0.4.0) was the inclusion of an [HDF5](https://www.hdfgroup.org/HDF5/) module, which provides functions to store pampro objects inside HDF5 containers. This has numerous advantages, the most important being incredibly fast loading times for triaxial Time Series data (<10 seconds for a week long 100 Hz file). It also provides a mechanism to automatically cache the results of various time consuming functions, such as nonwear detection and autocalibration; this means if a second analysis is performed, the results will be instantly recalled from storage. In the future, it will become a low-memory solution for pampro analyses, which will minimise the RAM footprint by writing results directly to disk.
