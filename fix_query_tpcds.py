#!/usr/bin/env python3
import os

# Loop over file numbers 1 to 99
for i in range(1, 100):
    filename = f"query{i}.tpl"
    # Check if the file exists
    if os.path.isfile(filename):
        # Read the original content of the file
        with open(filename, "r", encoding="utf-8") as file:
            content = file.read()
        # Insert the desired line at the beginning
        new_content = 'define _END = "";\n' + content
        # Write the new content back to the file
        with open(filename, "w", encoding="utf-8") as file:
            file.write(new_content)
        print(f"Updated {filename}")
    else:
        print(f"{filename} does not exist")
