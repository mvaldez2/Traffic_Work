# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 13:48:13 2019

@author: mvaldez2
"""

import os
import glob
import subprocess
import re
from datetime import datetime


path = 'T:/SR2/logs/processed2'
extension = 'csv'
os.chdir(path) 

current_day = datetime.now().day
current_month = datetime.now().month
current_year = datetime.now().year

import datetime
current_week = datetime.date(current_year, current_month, current_day).isocalendar()[1]

#looks for data from the current month to process
for infile in glob.glob('*.{}'.format(extension)):
    date = (re.findall('\d+', infile))
    week = datetime.date(int(date[0]), int(date[1]), int(date[2])).isocalendar()[1]
    if week >= current_week:
        command = ('python T:/Programs/Miguel/Done/combined_scripts_dir.py T:/SR2/logs/processed2/'+infile+' done')
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        print("Included: " + infile)
    else:
        print("Not included: " + infile)
    