import ftplib
import zipfile
import fnmatch
import os
import logging
import subprocess

name = 'ftpcon.log'

ftpserver = '' # server name
ftpuser = '' #Username
ftppassword= '' #password

directory =""  #   Directory i want to download files from, can be changed or left for user input
filematch = '*.zip..mongo'  #  A match for any file in this case, can be changed or left for user to input

# rootPath = "/tmp/vk_tmp/input_actual/" # On the cluster
rootPath = "" # Local Directory where you want data
pattern = '*.zip.mongo' #Pattern. This can be the same thing as the previous one

local_data_directory = '' #Directoy at which you want all the data to be unzipped and kept to refer it to upload it into hadoop

try:
    os.remove(name)
except OSError:
    pass

logging.basicConfig(filename=name, level=logging.INFO)

passed_files_ftp = 0
failed_files_ftp = 0
total_files_ftp = 0

total_files_unzip = 0
passes_files_unzip = 0
failed_files_unzip = 0

#Open ftp connection
ftp = ftplib.FTP_TLS(ftpserver, ftpuser,
ftppassword)

ftp.prot_p()    # securing the ftp connection

# os.chdir("c:/Users/USER/Desktop/new") #changes the active dir - this is where downloaded files will be saved to

#List the files in the current directory - TESTING THE CONNECTION
print "File List:" 
files = ftp.dir()
print files


ftp.cwd(directory)

for filename in ftp.nlst(filematch):    # Loop - looking for matching files
    total_files_ftp += 1
    try:
        fhandle = open(filename, 'wb')
        print 'Getting ' + filename     # For comfort sake, shows the file that's being retrieved
        ftp.retrbinary('RETR ' + filename, fhandle.write)
        fhandle.close()
        passed_files_ftp += 1
    except:
        print filename + ' cannot be opened'
        logging.info(filename + ' could not be ftp transferred')
        failed_files_ftp += 1

# LOGGING ADDED FOR REPORTING PURPOSES
failed_percent_ftp = failed_files_ftp/total_files_ftp * 100
logging.info('Number of failed files: ' + str(failed_files_ftp))
logging.info('Percent of failure: ' + str(failed_percent_ftp))
logging.info('######################END OF FTP##############################')

print "Closing the ftp connection"
ftp.close()

for root, dirs, files in os.walk(rootPath):
    for filename in fnmatch.filter(files, pattern):
        print(os.path.join(root, filename))
        total_files_unzip += 1
        try:
            passes_files_unzip += 1
            zipfile.ZipFile(os.path.join(root, filename)).extractall(local_data_directory)
            os.remove(os.path.join(root, filename))
        except:
            logging.info(filename + ' could not be unzipped')
            failed_files_unzip += 1
#
#
# LOGGING ADDED FOR REPORTING PURPOSES
failed_percent_unzipping = failed_files_unzip/total_files_unzip * 100
logging.info('Number of failed files: ' + str(failed_files_unzip))
logging.info('Percent of failure: ' + str(failed_percent_unzipping))
logging.info('######################END OF UNZIPPING##############################')

subprocess.call("/Users/krishnateja/Projects/FTP-connections/test.sh")
# https://aws.amazon.com/articles/3998 --> Using amazon s3 and arciving the files.