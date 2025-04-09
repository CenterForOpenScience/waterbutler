source_file = "compat.py"
target_file = "/usr/local/lib/python3.13/site-packages/boto/compat.py"

with open(source_file, "r") as src:
    content = src.read()

with open(target_file, "w") as dst:
    dst.write(content)


source_file = "plugin.py"
target_file = "/usr/local/lib/python3.13/site-packages/boto/plugin.py"

with open(source_file, "r") as src:
    content = src.read()

with open(target_file, "w") as dst:
    dst.write(content)


