import pandas as pd
from matplotlib.pyplot import step, show
pd.options.display.max_columns = 50


data = pd.read_csv("2019_07_03.csv", header=0)#, skiprows=range(1,8))  # row skip unneeded if using processed files

# Fix timestamps to be in proper format
data['Timestamp'] = pd.to_datetime(data['Timestamp'], format="%m/%d/%Y %H:%M:%S.%f")
data = data.loc[data['Event Type'].isin(range(81,100))]

def vc(df, pod1, pod2):
    cols = ['Timestamp', 'Event Type', 'Parameter', 'pod']
    channel = []
    df = df.loc[df['Parameter'].isin([pod1, pod2])]
    df['Next Event Type'] = df['Event Type'].shift(-1).fillna(0).astype(int)
    df['Next Parameter'] = df['Parameter'].shift(-1).fillna(0).astype(int)
    df['Last Parameter'] = df['Parameter'].shift(1).fillna(0).astype(int)
    df['Next Timestamp'] = df['Timestamp'].shift(-1)
    df['Last Timestamp'] = df['Timestamp'].shift(1)
    for index, row in df.iterrows():
        if (row['Timestamp'] == row['Next Timestamp']) and (row['Event Type']==82 and row['Next Event Type']==81):
                pass
        elif row['Parameter'] == row['Next Parameter'] and row['Event Type'] == 82:
            channel.append([row['Timestamp'], 82, 'VC ' +str(pod1)+ '/' + str(pod2), row.Parameter])
        elif row['Parameter'] == row['Last Parameter'] and row['Event Type'] == 81:
            channel.append([row['Timestamp'], 81, 'VC ' +str(pod1)+ '/' + str(pod2), row.Parameter])
        elif row['Event Type'] == 82 and row['Parameter'] == pod1:
            channel.append([row['Timestamp'], 82, 'VC ' +str(pod1)+ '/' + str(pod2), row.Parameter])
        elif row['Event Type'] == 81 and row['Parameter'] == pod1:
            channel.append([row['Next Timestamp'], 81, 'VC ' +str(pod1)+ '/' + str(pod2), row.Parameter])        
            
    df = pd.DataFrame(channel, columns=cols)
    df = df.sort_values(by=['Timestamp'])
    df['Next Timestamp'] = df['Timestamp'].shift(-1)
    df['Last Timestamp'] = df['Timestamp'].shift(1)
    df['Next Event Type'] = df['Event Type'].shift(-1).fillna(0).astype(int)
    df['Next2 Event Type'] = df['Event Type'].shift(-2).fillna(0).astype(int)
    df['Last Event Type'] = df['Event Type'].shift(1).fillna(0).astype(int)
    df['Next pod'] = df['pod'].shift(-1).fillna(0).astype(int)
    df['Next2 pod'] = df['pod'].shift(-2).fillna(0).astype(int)
    df['Last pod'] = df['pod'].shift(1).fillna(0).astype(int)
    
    mark = []    
    for index, row in df.iterrows():
        if row.Timestamp == row['Next Timestamp'] and (row['Event Type']==81 and row['Next Event Type']==82):
            mark.append(False)
        elif row.Timestamp == row['Last Timestamp'] and (row['Event Type']==82 and row['Last Event Type']==81):
            mark.append(False)
        elif (row['Event Type'] == 81 and row['Next Event Type'] == 81) and row.Timestamp == row['Next Timestamp']:
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
    for index, row in df.iterrows():
        if row['Last pod'] != row['pod'] and (row['Event Type'] == 82 and row['Last Event Type'] == 82) and row['Next pod'] == row.pod and (row['Next2 pod'] == row.pod):
            during.append('during')
        #trying to create a stopping point
        elif row['Event Type'] == 82 and row['Last pod'] == row.pod and (row['Next pod'] != row.pod and row['Next Event Type'] == 81):
            during.append('stop during')
        else:
            during.append('normal')
    df['during'] = during
        
    
    
#    i=0
#    while i < 20:
#        df['Next during'] = df['during'].shift(-1)
#        df['Last during'] = df['during'].shift(1)
#        df['Next Keep'] = df['Keep'].shift(-1)
#        df['Last Keep'] = df['Keep'].shift(1)
#        for index, row in df.iterrows():
#            if row.during == 'normal' and row['Last during'] == 'during' and row['Next during'] == 'normal' and row['Next Keep'] != False:
#                df.at[index,'during'] = 'during'
#        i += 1    
            
    
    #df = df[['Timestamp', 'Event Type', 'Parameter', 'during', 'Keep']]
    
    df = df.loc[df.Keep==True, :]
    df = df[['Timestamp', 'Event Type', 'Parameter']] 
    
    df.to_csv('pods' + str(pod1) + '-' + str(pod2) + '-virtualchannel.csv')
    #df.to_csv('test.csv')
    return df        
    
def compare(data, vc, pod, start, end):
    data = data.set_index('Timestamp').between_time(start,end).reset_index()
    vc = vc.set_index('Timestamp').between_time(start,end).reset_index()
    data = data.loc[(data['Event Type'].isin([81,82]))]
    data = data.loc[data.Parameter == pod,:] #gets pod detectors
    step(vc.Timestamp, vc['Event Type']) #graphs on/off of loop detectors
    step(data.Timestamp, data['Event Type']) #graphs on/off of pod detectors
    show()
    
a = vc(data, 13, 34)
b = vc(data, 14, 35)
c = vc(data, 17, 36)

compare(data, a, 64, '12:00', '12:05')
compare(data, b, 63, '12:00', '12:05')
compare(data, c, 62, '12:00', '12:05')
    
    
    
    
    
    
    
