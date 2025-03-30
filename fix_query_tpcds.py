import os

for i in range(1, 100):
    filename = f"query{i}.tpl"
    if os.path.isfile(filename):
        with open(filename, "r", encoding="utf-8") as file:
            content = file.read()
        new_content = 'define _END = "";\n' + content
        with open(filename, "w", encoding="utf-8") as file:
            file.write(new_content)
        print(f"Updated {filename}")
    else:
        print(f"{filename} does not exist")
