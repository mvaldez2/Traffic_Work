# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 12:23:15 2019

@author: mvaldez2
"""

import subprocess

command = ('for %f in (*.csv); do python T:/Programs/Miguel/test2/combined_scripts_dir.py %f done')
process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, shell=True)
output, error = process.communicate()