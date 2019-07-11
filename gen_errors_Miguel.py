# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 08:17:28 2019

@author: dhersch1
"""

## Import necessary modules

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import argparse
import datetime

## Argument Parsing

## Instantiate the parser
#parser = argparse.ArgumentParser(description='Takes traffic data file as input and outputs errors')
#
## Required positional argument
#parser.add_argument('infile', type=str,
#                    help='Name of file to be processed')
#
#args = parser.parse_args()

## Import Data and Preprocess

#root_dir = args.infile

data = pd.read_csv("2019_7_3_gen_errors.csv", header=0)#, skiprows=range(1,8))  # row skip unneeded if using processed files
data['Timestamp'] = pd.to_datetime(data.Timestamp)
#display(data.head())


# Find only Event types 81, 82 for on/off and 1, 7 for green light events
data = data.loc[data['Event Type'].isin([81, 82, 1, 7])]


#add if green column
lights = []
#finds starting and end points for green light events
for index, row in data.iterrows():
    if row['Event Type'] == 1 and row['Parameter']==2:
        lights.append('Green Start')
    elif row['Event Type'] == 7 and row['Parameter']==2:
        lights.append('Green End')
    else:
        lights.append('Red or Yellow')
 
data['Light'] = lights 

data['Next Light'] = data['Light'].shift(-1)
data['Last Light'] = data['Light'].shift(1)
during = []
#checks if row is during green
for index, row in data.iterrows():
    if (row['Event Type'] == 81 or row['Event Type'] == 82) and row['Next Light'] != 'Green Start' and row['Last Light']!= 'Green Start':
        during.append('Not Green')
    elif  (row['Event Type'] == 81 or row['Event Type'] == 82) and row['Next Light'] == 'Green Start':
         during.append('Not Green')       
    else:
       during.append('During Green') 
data['During'] = during

#finds rows between start and end points
i = 0
while i <10:
    data['Last During'] = data['During'].shift(1)
    for index, row in data.iterrows():
        if (row['Event Type'] == 81 or row['Event Type'] == 82) and row['Last During'] == 'During Green' and (row['Last Light'] != 'Green End' ): 
            data.at[index,'During'] = 'During Green'
    i =  i + 1
        

## Error generation function

def gen_errors(loop_ch, pod_ch, df):
    # Copy data and create offset columns for each of original columns
    df_test = df.copy()
    df_test = df_test.loc[df_test['Parameter'].isin([loop_ch, pod_ch])]
    #display(df_test.head())
    
    # Shift each one down by negative 1
    df_test['Next Timestamp'] = df_test['Timestamp'].shift(-1)
    df_test['Next Event Type'] = df_test['Event Type'].shift(-1).fillna(0).astype(int)
    df_test['Next Parameter'] = df_test['Parameter'].shift(-1).fillna(0).astype(int)
    #display(df_test.head())
    
    
    
    ## SET VARIABLES TO KEEP TRACK OF
    errors = pd.DataFrame(columns=['Start Time', 'Duration', 'Error Type', 'Light'])
    missed_call_ct = 0
    false_call_ct = 0
    
    ## KEEPING TRACK OF LOOP STATE AND POD STATE FOR MISSED AND FALSE CALLS
    loop_search = True
    pod_search = True
    
    # Find the first value in the dataframe for the loop_ch and pod_ch.  This will allow us to determine its starting state.
    for index, row in df_test.iterrows():
        
        if loop_search and (row['Parameter'] == loop_ch):
            if (row['Event Type'] == 81):
                loop_state = True # Establish loop starting state as ON, because first detector event is OFF
            elif (row['Event Type'] == 82):
                loop_state = False # Establish loop starting state as OFF, because first detector event is ON
                
            loop_search = False # Stop searching for loop starting state
            
        if pod_search and (row['Parameter'] == pod_ch):
            if (row['Event Type'] == 81):
                pod_state = True # Establish pod starting state as ON, because first detector event is OFF
            elif (row['Event Type'] == 82):
                pod_state = False # Establish pod starting state as OFF, because first detector event is ON
                
            pod_search = False # Stop searching for pod starting state
            
        if (not loop_search) and (not pod_search):
            break

    ## GENERATION OF ERRORS
    for index, row in df_test.iterrows():
        
        duration = datetime.timedelta(0)
        error_type = ''

        if index == df_test.shape[0]-1:
            break

        new_row = []

        # L1V0 positive activation
        if (row['Parameter'] == loop_ch) and (row['Event Type'] == 82): # loop turns on

            loop_state = True
            
            start_time = row['Timestamp']

            if (row['Next Parameter'] == pod_ch) and (row['Next Event Type'] == 82): # pod turns on

                duration = row['Next Timestamp'] - start_time
                error_type = 'L1P0'
                
            # Missed activation call
            elif (row['Next Parameter'] == loop_ch) and (row['Next Event Type'] == 81): # loop turns off before pod does anything
                # if pod already on, then error will be caught in the before or after step
                # if pod off, then missed activation call
                if pod_state:
                    pass
                else:
                    missed_call_ct += 1

        # L0V1 negative activation
        elif (row['Parameter'] == pod_ch) and (row['Event Type'] == 82): # pod turns on
            
            pod_state = True

            start_time = row['Timestamp']

            if (row['Next Parameter'] == loop_ch) and (row['Next Event Type'] == 82): # loop turns on

                duration = row['Next Timestamp'] - start_time
                error_type = 'L0P1'
                
            # False activation call
            elif (row['Next Parameter'] == pod_ch) and (row['Next Event Type'] == 81): # pod turns off before loop does anything
                # if loop already on, then error will be caught in the before or after step
                # if loop off, then false activation call
                if loop_state:
                    pass
                else:
                    false_call_ct += 1

        # L0V1 positive termination
        elif (row['Parameter'] == loop_ch) and (row['Event Type'] == 81): # loop turns off
            
            loop_state = False

            start_time = row['Timestamp']

            if (row['Next Parameter'] == pod_ch) and (row['Next Event Type'] == 81): # pod turns off

                duration = row['Next Timestamp'] - start_time
                error_type = 'L0P1'
                
            # Missed termination call
            elif (row['Next Parameter'] == loop_ch) and (row['Next Event Type'] == 82): # loop turns on before pod does anything
                # if pod already off, then error will be caught in the before or after step
                # if pod on, then missed termination call
                if pod_state:
                    missed_call_ct += 1
                else:
                    pass

        # L1V0 negative termination
        elif (row['Parameter'] == pod_ch) and (row['Event Type'] == 81): # pod turns off
            
            pod_state = False

            start_time = row['Timestamp']

            if (row['Next Parameter'] == loop_ch) and (row['Next Event Type'] == 81): # loop turns off

                duration = row['Next Timestamp'] - start_time
                error_type = 'L1P0'
                
            # False termination call
            elif (row['Next Parameter'] == pod_ch) and (row['Next Event Type'] == 82): # pod turns on before loop does anything
                # if loop already off, then error will be caught in the before or after step
                # if loop on, then false termination call
                if loop_state:
                    false_call_ct += 1
                else:
                    pass

        if duration.total_seconds() != 0:
        
            #print(start_time, duration, error_type)
            new_row = pd.Series([start_time, duration.total_seconds(), error_type, row['During']], index=errors.columns)
            #print(new_row)
            errors = errors.append(new_row, ignore_index=True)
            #errors.append({'Start Time': start_time, 'Duration': duration, 'Error Type': error_type}, ignore_index=True)
            
        else:
            pass
        
    return errors

## Running error generation for all pairs, export .csv and .pdf

# Create directory to save error .csv and graphs to:
date_string = str(str(data['Timestamp'].iloc[0].year)+'_'+
                  str(data['Timestamp'].iloc[0].month)+'_'+
                  str(data['Timestamp'].iloc[0].day))

save_dir = date_string

if not os.path.exists(save_dir):
    os.makedirs(save_dir)

# First, create a list of paired loop, pods (currently using virtual channels)
list_of_looppod_pairs = [[1334,64], [1435, 63], [1736, 62]]

error_85 = pd.DataFrame(columns=['Type', '85th percentile error'])



combined_error = pd.DataFrame()

for i in range(len(list_of_looppod_pairs)):
    pair = list_of_looppod_pairs[i]
    errors = gen_errors(pair[0], pair[1], data)
    error_filter = errors.loc[((errors['Light'] == 'Not Green' )& (errors['Duration'] > 5)) | ((errors['Light'] == 'During Green') & (errors['Duration'] > 1))]
    error_filter.to_csv(save_dir+'\\'+
                  date_string+
                  '_above_threshold_'+'LoopCh'+str(pair[0])+'_'+'PodCh'+str(pair[1])+'.csv')
        
    errors = errors.loc[((errors['Light'] == 'Not Green' )& (errors['Duration'] < 5)) | ((errors['Light'] == 'During Green') & (errors['Duration'] < 1))]
    errors.to_csv(save_dir+'\\'+
                  date_string+
                  '_'+'LoopCh'+str(pair[0])+'_'+'PodCh'+str(pair[1])+'.csv')
    combined_error = combined_error.append(errors)
    
    
    ## 85th Percentile Errors    
    #reset df
    error_85 = error_85[0:0]
    
    # Overall error
    error_85.loc[-1] = ['Overall',  errors['Duration'].quantile(0.85) ]  
    error_85.index = error_85.index + 1  
    error_85 = error_85.sort_index()
      
    # During Green error
    during_green = errors.loc[errors.Light=='During Green', :]
    error_85.loc[-1] = ['During Green',  during_green['Duration'].quantile(0.85) ]  
    error_85.index = error_85.index + 1  
    error_85 = error_85.sort_index()    
    
    # Not Green error
    not_green = errors.loc[errors.Light=='Not Green', :]
    error_85.loc[-1] = ['Not Green',  not_green['Duration'].quantile(0.85) ]  
    error_85.index = error_85.index + 1  
    error_85 = error_85.sort_index() 
    
    # L1P0 Green error
    L1P0 = errors.loc[errors['Error Type']=='L1P0', :]
    error_85.loc[-1] = ['L1P0',  L1P0['Duration'].quantile(0.85) ]  
    error_85.index = error_85.index + 1  
    error_85 = error_85.sort_index()
    
    # L0P1 Green error
    L0P1 = errors.loc[errors['Error Type']=='L0P1', :]
    error_85.loc[-1] = ['L0P1',  L0P1['Duration'].quantile(0.85) ]  
    error_85.index = error_85.index + 1  
    error_85 = error_85.sort_index()          
        
    error_85.to_csv(save_dir+'\\'+
                  date_string+
                  '_'+'85th_error_'+'LoopCh'+str(pair[0])+'_'+'PodCh'+str(pair[1])+'.csv')
    
    
    colors = []
    
    for index, row in errors.iterrows():
        if row['Error Type'] == 'L0P1':
            colors.append('b')
        else:
            colors.append('r')
    
    fig = plt.figure(figsize=[24, 11.25])
    plt.bar(list(errors['Start Time']), list(errors['Duration']), width=16./24/60/60, color=colors)
    plt.title('Duration vs. Start Time for L0P1 and L1P0 Errors \n\n'+'Loop, channel '+str(pair[0])+', and pod, channel '+str(pair[1])+'\n\n' + date_string)
    plt.xlabel('Time')
    plt.ylabel('Duration (in seconds)')
  #  plt.xlim(0:00,24:00)
    #plt.show()
    fig.savefig(save_dir+'\\'+
                date_string+
                '_'+'LoopCh'+str(pair[0])+'_'+'PodCh'+str(pair[1])+'.svg', 
                format='svg')


      
        
        
        