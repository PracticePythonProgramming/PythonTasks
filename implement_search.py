#!/usr/bin/env python3

import search_time_complexity as tc
import matplotlib.pyplot as plt
import algorithms as alg

#=================================================================================================
# Initialise a list to reference the appropriate algorithm.
# Initialise a dictionary to store the experimental results.
#=================================================================================================

search_algorithm = ['Linear Search','Binary Search']
table_of_results = {'Linear Search':{},'Binary Search':{}}
#=================================================================================================
# We make use of the functions found in the search_time_complexity package to generate experimental 
# results that will be used for plotting the graphs.
#=================================================================================================

def generate_experiment_results(search_algorithm,table_of_results,STEP=1000, REPS_PER_ARR=1000, MAX_ARR_LEN=10000):
    if search_algorithm == 'Linear Search':
        exec_times, len_of_arrays = tc.run_experiment(alg.linear_search,search_algorithm,STEP,REPS_PER_ARR,MAX_ARR_LEN)
    elif search_algorithm == 'Binary Search':
        exec_times, len_of_arrays = tc.run_experiment(alg.binary_search,search_algorithm,STEP,REPS_PER_ARR,MAX_ARR_LEN)

    table_of_results[search_algorithm]['exec_times'] = exec_times
    table_of_results[search_algorithm]['len_of_arrays'] = len_of_arrays
    return table_of_results

#=================================================================================================
# Execute the function defined above to populate the dictionary with experimental results.
#=================================================================================================
table_of_results = generate_experiment_results('Linear Search',table_of_results)
table_of_results = generate_experiment_results('Binary Search',table_of_results)
