# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 12:41:04 2019

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
# Matplotlib as Pyplot
import matplotlib.pyplot as plt
# RegEx
import re
# Bokeh
from bokeh.models import ColumnDataSource, Plot, LinearAxis, Grid
from bokeh.models.glyphs import Step
from bokeh.io import curdoc, show

## Argument Parsing

# Instantiate the parser
parser = argparse.ArgumentParser(description='Takes traffic data file as input and outputs errors')

# Required positional argument
parser.add_argument('infile', type=str,
                    help='Name of file to be processed')
parser.add_argument('start_time', type=str,
                    help='Start of time period of interest, as hh:mm')
parser.add_argument('end_time', type=str,
                    help='Start of time period of interest, as hh:mm')

args = parser.parse_args()

## Import Data and Preprocess

root_dir = args.infile

filename_parsed = re.split('[_.]', args.infile)
year = int(filename_parsed[0][-4:])
month = int(filename_parsed[1])
day = int(filename_parsed[2])

start_time_parsed = re.split(':', args.start_time)
start_time_h = int(start_time_parsed[0])
start_time_m = int(start_time_parsed[1])
end_time_parsed = re.split(':', args.end_time)
end_time_h = int(end_time_parsed[0])
end_time_m = int(end_time_parsed[1])

start_time = datetime.datetime(year, month, day, start_time_h, start_time_m)
end_time = datetime.datetime(year, month, day, end_time_h, end_time_m)

data = pd.read_csv(root_dir, header=0)#, skiprows=range(1,8))  # row skip unneeded if using processed files

# Fix timestamps to be in proper format
data['Timestamp'] = pd.to_datetime(data['Timestamp'], format="%m/%d/%Y %H:%M:%S.%f")
#display(data.head())

# Find only Event types 81-100, and Parameters of either 13, 14, 10, 17 and 53-54, 51-52, 55-56, 49-50
data = data.loc[data['Event Type'].isin(range(81,100))]
channel_nos = [10,13,14,17,49,50,51,52,53,54,55,56]
data = data.loc[data['Parameter'].isin(channel_nos)]

data = data.loc[(data['Timestamp'] >= start_time) &
                (data['Timestamp'] <= end_time)]
# Alternatively, could plot the entire time, and then slice the graph by time?

#%%
# Plot On/Off for head of each dataset
fig = plt.figure(figsize=[6.4*3+1,3*4.8])
ax = plt.subplot(111)

for channel in channel_nos:

    if channel <= 17:
        plt.step(data['Timestamp'].loc[data['Parameter'].isin([channel])], data['Event Type'].loc[data['Parameter'].isin([channel])]-channel-30, label=str('Channel ' + str(channel)), linewidth=0.75)
    else:
        plt.step(data['Timestamp'].loc[data['Parameter'].isin([channel])], data['Event Type'].loc[data['Parameter'].isin([channel])]-channel, label=str('Channel ' + str(channel)), linewidth=0.75)

# Shrink current axis's height by 10% on the bottom
box = ax.get_position()
ax.set_position([box.x0, box.y0 + box.height * 0.1,
                 box.width, box.height * 0.9])

# Put a legend below current axis
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5),
          fancybox=True, shadow=True)

# Trying to get scaling right
#plt.autoscale()
#fig.tight_layout()
#ax.axis('scaled')

# Create directory to save error .csvs and graphs to:
date_string = str(str(data['Timestamp'].iloc[0].year)+'_'+
                  str(data['Timestamp'].iloc[0].month)+'_'+
                  str(data['Timestamp'].iloc[0].day))

# save_dir = 'T:\SR2\errors\\'+date_string
save_dir = './'

fig.savefig(save_dir+date_string+
            '_'+args.start_time.replace(':', '')+
            '_'+args.end_time.replace(':', '')+
            '_step_plots'+'.svg',
            format='svg')


fig.show()
