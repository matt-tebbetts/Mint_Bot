import csv

# Define the input and output file paths
input_csv_path = 'transactions.csv'
output_csv_path = 'test.csv'

# Open the input file in read mode and the output file in write mode
with open(input_csv_path, mode='r', encoding='utf-8') as infile, \
     open(output_csv_path, mode='w', newline='', encoding='utf-8') as outfile:
    
    # Create a csv reader and writer
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    # Read the headers and the first record
    headers = next(reader)  # This reads the first line (headers)
    first_record = next(reader, None)  # This attempts to read the next line (first record)
    
    # Write the headers and the first record to the output file
    writer.writerow(headers)
    if first_record:
        writer.writerow(first_record)
