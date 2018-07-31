# -*- coding: utf-8 -*-
"""
Created on Tue Jul 31 09:50:00 2018

@author: Jin
"""

import sys

def update_progress(progress, label='Processing'):
    """
        progress bar.
        using this custom progress bar since it is difficult to control the 
        output of existing progressbar libs., e.g., tqdm
        
        params
        ========
        progress float >=0 and <= 1
        label str the text before the progress bar        
        
    """
    bar_length = 50 # the length of the progress bar
    status = ""
    if 0<=progress<=1:
        step = int(round(bar_length*progress))
        ani="#"*step + "-"*(bar_length-step)
        if progress == 1: status = 'Done.'        
        text = f"\r{label}: [{ani}] {round(progress*100,2)}% {status}"
        sys.stdout.write(text)
        sys.stdout.flush()
    else:
        print('progress must be within [0, 1]')
        
        
if __name__=='__main__':
    pass