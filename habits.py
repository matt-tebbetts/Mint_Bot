import os
import pandas as pd

# Setup paths
my_dir = 'files/habits'
latest_directory = max(
    (d for d in os.listdir(my_dir) if os.path.isdir(os.path.join(my_dir, d))),
    key=lambda x: os.path.getmtime(os.path.join(my_dir, x))
)
base_path = os.path.join(my_dir, latest_directory)

# Process Checkmarks.csv
df = (
    pd.read_csv(f'{base_path}/Checkmarks.csv')
    .set_index('Date')
    .transpose()
    .reset_index()
    .rename(columns={'index': 'Habit'})
    .melt(id_vars='Habit', var_name='Date', value_name='Value')
)

# Load value_xref.csv and merge to translate 'Value' to 'Completed'
df['Value'] = df['Value'].astype(float)
value_xref = pd.read_csv(f'{my_dir}/value_xref.csv')
value_xref['Value'] = value_xref['Value'].astype(float)
df = df.merge(value_xref, on='Value', how='left')

# Merge with Habits.csv
df = (
    df.merge(pd.read_csv(f'{base_path}/Habits.csv')[['Name', 'Question']],
             left_on='Habit', right_on='Name')
    .drop(columns='Name')
)

# Reorder columns
df = df[['Habit', 'Date', 'Value', 'Question', 'Completed']]

df.to_csv(f'{my_dir}/latest.csv', index=False)
