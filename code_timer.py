import time
import pandas as pd

def timer_start():
    start_time = time.time()
    return start_time

def timer_end(bgn):
    end = time.time()
    sec = end - bgn
    df = pd.DataFrame({
        'run_bgn': [bgn],
        'run_end': [end],
        'run_sec': [sec]
    })
    return df

bgn = timer_start()

# do stuff here
# 

time_df = timer_end(bgn)
