import os

def read_text_files(directory):
    # List all files in the directory
    files = os.listdir(directory)
    # Filter out non-txt files
    txt_files = [f for f in files if f.endswith('.txt')]
    
    for file in txt_files:
        file_path = os.path.join(directory, file)
        with open(file_path, 'r') as f:
            content = f.read()
            print(f'Content of {file}:')
            print(content)
            print('---')

# Specify the directory containing the txt files
directory = 'C:\\teste'

read_text_files(directory)