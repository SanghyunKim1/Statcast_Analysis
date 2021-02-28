import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import glob, os
import missingno as msno
import warnings
warnings.filterwarnings('ignore')
from matplotlib.axes._axes import _log as matplotlib_axes_logger
matplotlib_axes_logger.setLevel('ERROR')

# load data
path = '/Users/sanghyunkim/Desktop/Data Science Project/MLB Analysis/Statcast_Analysis/data'
all_files = glob.glob(os.path.join(path, '*.csv'))
statcast_df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

# data structure
print(statcast_df.shape)
print(statcast_df.dtypes.to_string())

# change data types
obj_cols = ['batter', 'pitcher', 'zone', 'hit_location', 'balls', 'strikes',
            'game_year', 'on_3b', 'on_2b', 'on_1b', 'outs_when_up', 'inning',
            'fielder_2', 'fielder_3', 'fielder_4', 'fielder_5', 'fielder_6',
            'fielder_7', 'fielder_8', 'fielder_9', 'sv_id', 'game_pk', 'launch_speed_angle']
statcast_df[obj_cols] = statcast_df[obj_cols].astype(str)

# check missing data
missing_cols = statcast_df.columns[statcast_df.isnull().any()]
print(statcast_df[missing_cols].isnull().sum())

msno.matrix(statcast_df[missing_cols], labels=True, fontsize=12, figsize=(25, 20))
plt.show()

missing_df = pd.DataFrame(index=missing_cols)
missing_df['% Missing Data'] = round(statcast_df[missing_cols].isnull().sum()/len(statcast_df[missing_cols]), 3)
print(missing_df.sort_values('% Missing Data', ascending=False))