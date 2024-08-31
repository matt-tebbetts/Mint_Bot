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

# Define the habit replacements and their respective dates
habit_replacements = {
    'Screen Time old': {'new_habit': 'No Phone', 'date': '2023-10-16'},
    'Vaping old': {'new_habit': 'No Smoking', 'date': '2023-10-15'}
}

# Apply the habit replacements and updates
for old_habit, details in habit_replacements.items():
    new_habit, date = details['new_habit'], details['date']

    # Update 'Habit' and 'Value' for dates before the specified date
    condition_before_date = (df['Habit'] == old_habit) & (df['Date'] < date)
    df.loc[condition_before_date, 'Habit'] = new_habit
    df.loc[condition_before_date & (df['Value'].isin([0, 2])), 'Value'] = df['Value'].map({2: 0, 0: 2})

    # Remove rows where the old habit is present for dates on or after the specified date
    df = df[~((df['Habit'] == old_habit) & (df['Date'] >= date))]

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
