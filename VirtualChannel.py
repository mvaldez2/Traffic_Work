import pandas as pd
from matplotlib.pyplot import step, show

pd.options.display.max_columns = 50

data = pd.read_csv("2019_07_03.csv", header=0)

# Fix timestamps to be in proper format
data['Timestamp'] = pd.to_datetime(data.Timestamp)

#used to combine original dataframe with virtual channels
to_combine = data
#keeping track of used loops
loops = []

#gets date for filenames
date_string = str(str(data['Timestamp'].iloc[0].year)+'_' +
                  str(data['Timestamp'].iloc[0].month)+'_' +
                  str(data['Timestamp'].iloc[0].day))

#gets only on/off events
data = data.loc[data['Event Type'].isin(range(81, 100))]

'''
Gets two loops and combines them into one virtual channel. 
It outputs the virtual channel into a csv file.
df : dataframe
loop1: first loop
loop2: second loop
ex: vc(data, 13, 34)
usage: 
    if you are making several virtual channels you can assign them to variables
        ex: vc1334 = vc(data, 13, 34)
            vc1435 = vc(data, 14, 35)
            vc1736 = vc(data, 17, 36)
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

    #identifies starting points and points of rows to remove
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

    #finds rows between starting points and end points to mark for deletion
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
    df.to_csv('loops' + str(loop1) + '-' + str(loop2) + '-virtualchannel.csv')

    return df

'''
returns graph that compares the activity of a virtual channel and a pod detector
data : dataframe
vc: virtual channel
pod: pod detector
ex: compare(data, vc1334, 64, '14:00', '14:05')
'''
def compare(data, vc, pod, start, end):
    data = data.set_index('Timestamp').between_time(start, end).reset_index()
    vc = vc.set_index('Timestamp').between_time(start, end).reset_index()
    data = data.loc[(data['Event Type'].isin([81, 82]))]
    data = data.loc[data.Parameter == pod, :]  # gets pod detectors
    step(vc.Timestamp, vc['Event Type'])  # graphs on/off of loop detectors
    step(data.Timestamp, data['Event Type'])  # graphs on/off of pod detectors
    show()

'''
combines virtual channels into original dataframe and removes loops associated with
the virtual channels and outputs .csv file to run on gen_errors scripts which finds errors
*vc: virtual channels
ex: combine_df(vc1334, vc1435, vc1736)
'''
def combine_df(*vc):
    #excludes loops used in virtual channels
    combine = to_combine[~to_combine['Parameter'].isin([loops])]
    #combines virtual channels into original dataframe
    errors = combine.append([*vc])
    errors = errors.sort_values(by=['Timestamp'])
    #outputs .csv file to use with gen_errors script
    errors.to_csv(date_string + '_gen_errors.csv')    
    return errors


#-------------- For testing (remove if trying new virtual channels) --------------
vc1334 = vc(data, 13, 34)
vc1435 = vc(data, 14, 35)
vc1736 = vc(data, 17, 36)

combine_df(vc1334, vc1435, vc1736)

compare(data, vc1334, 64, '14:00', '14:05')
compare(data, vc1435, 63, '14:00', '14:05')
compare(data, vc1736, 62, '14:00', '14:05')
