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
from matplotlib.pyplot import step, show

root = Tk()
root.filename =  filedialog.askopenfilename(initialdir = "/",title = "Select file",filetypes = (("csv files","*.csv"),("all files","*.*")))
virtual_channel = pd.read_csv(root.filename, header=0)

# Fix timestamps to be in proper format
virtual_channel['Timestamp'] = pd.to_datetime(virtual_channel.Timestamp)

#used to combine original virtual_channelframe with virtual channels
to_combine = virtual_channel
#keeping track of used loops
loops = []

#gets date for filenames
date_string = str(str(virtual_channel['Timestamp'].iloc[0].year)+'_' +
                  str(virtual_channel['Timestamp'].iloc[0].month)+'_' +
                  str(virtual_channel['Timestamp'].iloc[0].day))

save_dir = 'T:\SR2\errors\\'+date_string

if not os.path.exists(save_dir):
    os.mkdir(save_dir)

#gets only on/off events
virtual_channel = virtual_channel.loc[virtual_channel['Event Type'].isin(range(81, 100))]

'''
Gets two loops and combines them into one virtual channel. 
It outputs the virtual channel into a csv file.
df : virtual_channelframe
loop1: first loop
loop2: second loop
ex: vc(virtual_channel, 13, 34)
usage: 
    if you are making several virtual channels you can assign them to variables
        ex: vc1334 = vc(virtual_channel, 13, 34)
            vc1435 = vc(virtual_channel, 14, 35)
            vc1736 = vc(virtual_channel, 17, 36)
    then you can use them on the combine_df() function to output a file to find errors
'''
def vc(df, loop1, loop2):
    #creates a parameter for virtual channel
    par = str(loop1) + str(loop2)
    par = int(par)

    #keeping track of loops being used
    loops.append(loop1)
    loops.append(loop2)

    #setting up columns to analyze
    cols = ['Timestamp', 'Event Type', 'Parameter', 'pod']
    channel = []
    df = df.loc[df['Parameter'].isin([loop1, loop2])]
    df['Next Event Type'] = df['Event Type'].shift(-1).fillna(0).astype(int)
    df['Next Parameter'] = df['Parameter'].shift(-1).fillna(0).astype(int)
    df['Last Parameter'] = df['Parameter'].shift(1).fillna(0).astype(int)
    df['Next Timestamp'] = df['Timestamp'].shift(-1)
    df['Last Timestamp'] = df['Timestamp'].shift(1)

    #finds the basic requirements for the possibility of a row being part of the virtual channel
    for index, row in df.iterrows():
        if (row['Timestamp'] == row['Next Timestamp']) and (row['Event Type'] == 82 and \
           row['Next Event Type'] == 81):
            pass
        elif row['Parameter'] == row['Next Parameter'] and row['Event Type'] == 82:
            channel.append([row['Timestamp'], 82, par, row.Parameter])
        elif row['Parameter'] == row['Last Parameter'] and row['Event Type'] == 81:
            channel.append([row['Timestamp'], 81, par, row.Parameter])
        elif row['Event Type'] == 82 and row['Parameter'] == loop1:
            channel.append([row['Timestamp'], 82, par, row.Parameter])
        elif row['Event Type'] == 81 and row['Parameter'] == loop1:
            channel.append([row['Next Timestamp'], 81, par, row.Parameter])

    df = pd.DataFrame(channel, columns=cols)
    df = df.sort_values(by=['Timestamp'])

    #setup for more in depth analysis
    df['Next Timestamp'] = df['Timestamp'].shift(-1)
    df['Last Timestamp'] = df['Timestamp'].shift(1)
    df['Next Event Type'] = df['Event Type'].shift(-1).fillna(0).astype(int)
    df['Next2 Event Type'] = df['Event Type'].shift(-2).fillna(0).astype(int)
    df['Last Event Type'] = df['Event Type'].shift(1).fillna(0).astype(int)
    df['Last2 Event Type'] = df['Event Type'].shift(2).fillna(0).astype(int)
    df['Next pod'] = df['pod'].shift(-1).fillna(0).astype(int)
    df['Next2 pod'] = df['pod'].shift(-2).fillna(0).astype(int)
    df['Next3 pod'] = df['pod'].shift(-3).fillna(0).astype(int)
    df['Last pod'] = df['pod'].shift(1).fillna(0).astype(int)


    mark = [] #keeps track of rows that are not in the virtual channel

    #finds occurrences when events happen at the same time 
    for index, row in df.iterrows():
        if row.Timestamp == row['Next Timestamp'] and (row['Event Type'] == 81
                                                       and row['Next Event Type'] == 82):
            if row.pod == row['Next pod'] and row['Last Event Type'] == 81:
                mark.append(True)
            else:
                mark.append(False)
        elif row.Timestamp == row['Last Timestamp'] and (row['Event Type'] == 82
                                                         and row['Last Event Type'] == 81):
            if row.pod == row['Last pod'] and row.pod == row['Next pod'] and \
                    row['Last2 Event Type'] == 81:
                mark.append(True)
            else:
                mark.append(False)
        elif (row['Event Type'] == 81 and row['Next Event Type'] == 81) and \
                row.Timestamp == row['Next Timestamp']:
            mark.append(False) 
        elif row['Event Type'] == 82 and row['Last Event Type'] == 82:
            mark.append(False)
        elif row['Event Type'] == 81 and row['Next Event Type'] == 81:
            mark.append(False)
        else:
            mark.append(True)

    df['Keep'] = mark

    during = []
    df['Last Keep'] = df['Keep'].shift(1)

    #identifies starting and end points of rows to remove
    for index, row in df.iterrows():
        if row['Last pod'] != row['pod'] and \
            (row['Event Type'] == 82 and row['Last Event Type'] == 82) and \
                (row['Next pod'] == row.pod and row['Next2 pod'] == row.pod and \
                    row['Next3 pod'] == row.pod):
            during.append('during')
        elif row['Event Type'] == 82 and row['Last pod'] == row.pod and \
            (row['Next pod'] != row.pod and row['Next Event Type'] == 81):
            during.append('stop during')
        else:
            during.append('normal')
    df['during'] = during

    #finds rows between starting and end points to mark for deletion
    i = 0
    while i < 20:
        df['Next during'] = df['during'].shift(-1)
        df['Last during'] = df['during'].shift(1)
        df['Next Keep'] = df['Keep'].shift(-1)
        df['Last Keep'] = df['Keep'].shift(1)
        for index, row in df.iterrows():
            if row.during == 'normal' and row['Last during'] == 'during' and \
                row['Next during'] == 'normal' and (row['Keep'] != False
                                                    and row['Last during'] == 'during'):
                df.at[index, 'during'] = 'during'
        i += 1

    #keeps rows that are a part of the virtual channel
    df = df.loc[(df.Keep == True) & (df.during == 'normal')] 

    df = df[['Timestamp', 'Event Type', 'Parameter']]

    #outputs virtual channel to .csv file
    df.to_csv(save_dir +'\\'+ 'loops' + str(loop1) + '-' + str(loop2) + '-virtualchannel.csv')

    return df

'''
returns graph that compares the activity of a virtual channel and a pod detector
virtual_channel : virtual_channelframe
vc: virtual channel
pod: pod detector
ex: compare(virtual_channel, vc1334, 64, '14:00', '14:05')
'''
def compare(virtual_channel, vc, pod, start, end):
    virtual_channel = virtual_channel.set_index('Timestamp').between_time(start, end).reset_index()
    vc = vc.set_index('Timestamp').between_time(start, end).reset_index()
    virtual_channel = virtual_channel.loc[(virtual_channel['Event Type'].isin([81, 82]))]
    virtual_channel = virtual_channel.loc[virtual_channel.Parameter == pod, :]  # gets pod detectors
    step(vc.Timestamp, vc['Event Type'])  # graphs on/off of loop detectors
    step(virtual_channel.Timestamp, virtual_channel['Event Type'])  # graphs on/off of pod detectors
    show()

'''
combines virtual channels into original virtual_channelframe and removes loops associated with
the virtual channels and outputs date_gen_errors.csv file to run on gen_errors scripts which finds errors
*vc: virtual channels
ex: combine_df(vc1334, vc1435, vc1736)
'''
def combine_df(*vc):
    #excludes loops used in virtual channels
    combine = to_combine[~to_combine['Parameter'].isin(loops)]
    #combines virtual channels into original virtual_channelframe
    errors = combine.append([*vc])
    errors = errors.sort_values(by=['Timestamp'])
    #outputs .csv file to use with gen_errors script
    errors.to_csv(save_dir +'\\'+date_string + '_gen_errors.csv')    
    return errors


#-------------- For testing (remove if trying new virtual channels) --------------
vc1334 = vc(virtual_channel, 13, 34)
vc1435 = vc(virtual_channel, 14, 35)
vc1736 = vc(virtual_channel, 17, 36)

data = combine_df(vc1334, vc1435, vc1736) #you have to run this funtion if you want to generate errors 

compare(virtual_channel, vc1334, 64, '14:00', '14:05')
compare(virtual_channel, vc1435, 63, '14:00', '14:05')
compare(virtual_channel, vc1736, 62, '14:00', '14:05')



# Find only Event types 81, 82 for on/off and 1, 7 for green light events
data = data.loc[data['Event Type'].isin([81, 82, 1, 7])]
data = data.loc[data['Parameter'].isin([1334, 1435, 1736, 62, 63, 64, 2])] #change if trying different loops and pods (DON'T remove 2)


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

save_dir = 'T:\SR2\errors\\'+date_string
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

# First, create a list of paired loop, pods (currently using virtual channels)
list_of_looppod_pairs = [[1334,64], [1435, 63], [1736, 62]] #change if trying different loops and pods

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


      
        
        
        