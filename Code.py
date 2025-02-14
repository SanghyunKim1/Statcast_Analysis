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
obj_cols = ['batter', 'pitcher', 'zone', 'hit_location', 'on_3b', 'on_2b', 'on_1b',
            'fielder_2', 'fielder_3', 'fielder_4', 'fielder_5', 'fielder_6',
            'fielder_7', 'fielder_8', 'fielder_9', 'sv_id', 'game_pk']
statcast_df[obj_cols] = statcast_df[obj_cols].astype(str)

# check missing data
missing_cols = statcast_df.columns[statcast_df.isnull().any()]
print('------- Number of Missing Data -------')
print(statcast_df[missing_cols].isnull().sum())

msno.matrix(statcast_df[missing_cols], labels=True, fontsize=12, figsize=(25, 20))
plt.show()
msno.bar(statcast_df[missing_cols], labels=True, fontsize=12, figsize=(25, 20))
plt.show()
msno.heatmap(statcast_df[missing_cols], labels=True, fontsize=12, figsize=(25, 20))
plt.show()

missing_df = pd.DataFrame(index=missing_cols)
missing_df['Ratio of Missingness'] = round(statcast_df[missing_cols].isnull().sum()/len(statcast_df[missing_cols]), 6)
print(missing_df.sort_values('Ratio of Missingness', ascending=False))
# based on types of missing data and the proportions of missingness in each column,
# drop data features that are not worth imputation
