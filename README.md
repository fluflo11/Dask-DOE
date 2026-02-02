# Dask-DOE

## Setup

Just pull the repo and open it in your favorite notebook editor. Make sure to have Dask before starting.
Be aware that you need to have at least 50GB of free disk space.

## Background

This code was made for the Design Of Experiments course at the CVUT-Faculty of Nuclear Sciences and Physical Engineering. It is a better version of a work that I made some months ago as a summer project at the University of Rennes - ISTIC.
This is basically a refactoring of the precedent code, applying 2k factorial design to it.

## DOE

I choosed to use 2k factorial design because it was the most intuitive way to do a benchmark and comparing Pandas and Dask.
The plan for the analysis of the results is rather simple : 
* Cleaning results by separating configs where OOM errors happen and successful configs, and applying a log transformation on the time series (may not be necessary but its a safety measure)
* Using the analysis of variance on a dataset composed only on the successful configurations (IE configurations without Pandas + big file size)
  * Saphiro-wilk to test residue normality
  * Residuals vs fitted plot to ensure that my model is "right" (linear variables + homoscedasticity + outliers)
  * Model refinement through backward elimination
* Analysing how well Dask scales


## Possible improvements

You may want to test it on different hardware configurations. To do this, you could add a "config" variable with "high-end" and "low-end" values, but it would be better to consider configurations as operators, and then use blocking. 

# Author
Florentin Royer
