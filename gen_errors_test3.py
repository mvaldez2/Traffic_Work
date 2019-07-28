# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 08:17:28 2019

@author: dhersch1
"""

## Import necessary modules

import pandas as pd
import os
import matplotlib.pyplot as plt
import datetime
from tkinter import filedialog
from tkinter import *

pd.options.display.max_columns = 50
root = Tk()
root.filename =  filedialog.askopenfilename(initialdir = "/",title = "Select file",filetypes = (("csv files","*.csv"),("all files","*.*")))

constant_on = .04
constant_off = .09

variable_onG = .03
variable_offG = .07
variable_on = .06
variable_off = .15

activation_diff_g = variable_onG
termination_diff_g = variable_offG
activation_diff = variable_on
termination_diff = variable_off


data = pd.read_csv(root.filename, header=0)#, skiprows=range(1,8))  # row skip unneeded if using processed files
data['Timestamp'] = pd.to_datetime(data.Timestamp)
#display(data.head())


# Find only Event types 81, 82 for on/off and 1, 7 for green light events
data = data.loc[data['Event Type'].isin([81, 82, 1, 7])]
data = data.loc[data['Parameter'].isin([10, 1334, 1435, 1736, 55, 62, 63, 64, 2])] #change if trying different loops and pods (DON'T remove 2)


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
while i <20:
    data['Last During'] = data['During'].shift(1)
    for index, row in data.iterrows():
        if (row['Event Type'] == 81 or row['Event Type'] == 82) and row['Last During'] == 'During Green' and (row['Last Light'] != 'Green End' ): 
            data.at[index,'During'] = 'During Green'
    i =  i + 1
        
data = data.loc[data['Event Type'].isin([81, 82])]




## Marking start and end for green light
start = []
data['Last During'] = data['During'].shift(1)
data['Next During'] = data['During'].shift(-1)
for index, row in data.iterrows():
    
    if row['During'] == 'During Green' and row['Last During'] == 'Not Green':
        start.append('Green Start')
    elif row['During'] == 'During Green' and row['Next During'] == 'Not Green':
        start.append('Green End')
    else:
        start.append('During Light')
data['Light'] = start

#loop through and mark rows that occurr 5 seconds from the start time
five = []
initial = datetime.datetime(2018, 1, 1, 0, 00)
for index, row in data.iterrows():
    difference = (row.Timestamp - initial).total_seconds()
    if row['Light'] == 'Green Start':
        five.append(True)
        initial = row.Timestamp
    elif initial.year != 2018 and row['During'] == 'During Green' and row['Light'] != 'Green Start' and difference <= 5 :
        five.append(True)
    else:
        five.append(False)
    
data['Five'] = five

## Applying time differences
diff = []
for index, row in data.iterrows():
    if row['Event Type'] == 82 and row.Parameter == 10 or row.Parameter == 55:
        diff.append(row.Timestamp - datetime.timedelta(seconds=activation_diff))
    elif row['Event Type'] == 81 and row.Parameter == 10 or row.Parameter == 55:
        diff.append(row.Timestamp - datetime.timedelta(seconds=termination_diff))
    elif row['Event Type'] == 82 and row['During'] == 'During Green' and row['Five'] == True:
        diff.append(row.Timestamp - datetime.timedelta(seconds=activation_diff))
    elif row['Event Type'] == 81 and row['During'] == 'During Green' and row['Five'] == True:
        diff.append(row.Timestamp - datetime.timedelta(seconds=termination_diff))
    elif row['Event Type'] == 82 and row['During'] == 'During Green':
        diff.append(row.Timestamp - datetime.timedelta(seconds=activation_diff_g))
    elif row['Event Type'] == 82 and row['During'] == 'Not Green':
        diff.append(row.Timestamp - datetime.timedelta(seconds=activation_diff))
    elif row['Event Type'] == 81 and row['During'] == 'During Green':
        diff.append(row.Timestamp + datetime.timedelta(seconds=termination_diff_g))
    elif row['Event Type'] == 81 and row['During'] == 'Not Green':
        diff.append(row.Timestamp + datetime.timedelta(seconds=termination_diff))
data['Timestamp'] = diff

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
    df_test['Last Timestamp'] = df_test['Timestamp'].shift(1)
    df_test['Last Event Type'] = df_test['Event Type'].shift(1).fillna(0).astype(int)
    df_test['Last Parameter'] = df_test['Parameter'].shift(1).fillna(0).astype(int)
    #display(df_test.head())
    
    
    
    ## SET VARIABLES TO KEEP TRACK OF
    errors = pd.DataFrame(columns=['Start Time', 'Event Type', 'Duration', 'Error Type', 'Light', '0 Error'])
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
        
        if (row['Event Type'] == 82 and row['Next Event Type'] == 82) and (row.Timestamp == row['Next Timestamp']) and (row.Parameter != row['Next Parameter']):
            same = True
#        elif (row['Event Type'] == 82 and row['Last Event Type'] == 82) and (row.Timestamp == row['Last Timestamp']) and (row.Parameter != row['Last Parameter']):   
#            same = True
        else:
            same = False
            

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
            new_row = pd.Series([start_time, row['Event Type'], duration.total_seconds(), error_type, row['During'], same], index=errors.columns)
            #print(new_row)
            errors = errors.append(new_row, ignore_index=True)
            #errors.append({'Start Time': start_time, 'Duration': duration, 'Error Type': error_type}, ignore_index=True)
        elif duration.total_seconds() == 0 and same == True:
            new_row = pd.Series([start_time, row['Event Type'], duration.total_seconds(), error_type, row['During'], same], index=errors.columns)
            
            errors = errors.append(new_row, ignore_index=True)
        else:
            pass
        
    return errors

## Running error generation for all pairs, export .csv and .pdf

# Create directory to save error .csv and graphs to:
date_string = str(str(data['Timestamp'].iloc[0].year)+'_'+
                  str(data['Timestamp'].iloc[0].month)+'_'+
                  str(data['Timestamp'].iloc[0].day))


if (termination_diff == .15):
    test = "test3_variable"
elif(termination_diff == .09):
    test = "test3_constant"
else:
    test ="test3"

save_dir = 'T:\SR2\errors\\'+date_string+test

if not os.path.exists(save_dir):
    os.makedirs(save_dir)

# First, create a list of paired loop, pods (currently using virtual channels)
list_of_looppod_pairs = [[10, 55], [1334,64], [1435, 63], [1736, 62]] #change if trying different loops and pods

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

    error_85.loc[-1] = ['0 Errors', errors['0 Error'].values.sum() ]    
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


      
        
        
        