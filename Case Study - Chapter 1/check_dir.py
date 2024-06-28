

# importing os module
import os

# importing shutil module
import shutil

# path
path = 'C:/ProjectWork/1SProject/Data'

# List files and directories

print("Before copying file:")
print(os.listdir(path))

# Source path
src = 'C:/ProjectWork/1SProject/Data/Source'

# Destination path
dest = 'C:/ProjectWork/1SProject/Data/Archive'

# Copy the content of
# source to destination

destination = shutil.move(src, dest, copy_function = shutil.copytree)


print("After copying file:")
print(os.listdir(path))

# Print path of newly
# created file
print("Destination path:", destination)
os.mkdir('C:/ProjectWork/1SProject/Data/Source')
print("Done")