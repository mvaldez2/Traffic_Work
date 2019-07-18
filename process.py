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

current_month = datetime.now().month

#looks for data from the current month to process
for infile in glob.glob('*.{}'.format(extension)):
    month = (re.findall('\d+', infile))
    month = int(month[1])
    if month >= current_month:
        command = ('python T:/Programs/Miguel/Done/combined_scripts_dir.py T:/Programs/Miguel/test2/'+infile+' done')
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, shell=True)
        output, error = process.communicate()
    else:
        print("Not included")
    