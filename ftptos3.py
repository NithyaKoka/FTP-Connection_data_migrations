__author__ = 'krishnateja'

import os
from boto.s3.key import Key
import glob
import boto
import time
import datetime
from subprocess import call, check_call
import shutil


current_working_directory = os.getcwd()

# TODO: make sure you change the extension.
extension = '/*.txt'

last_imported_data = open('last_imported_data.txt', 'w')
timeStamp_s3_folder = time.strftime('%Y%m%d')  # we are using the exact same key for the s3 folder structure

# FTP credentials
# ftpserver = ''  # server name
# ftpuser = ''  # Username
# ftppassword = ''  # password

# AWS credentials
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = '' #even with the slash it is fine
bucket_name = ''

conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
mybucket = conn.get_bucket(bucket_name)

k = Key(mybucket)

hadoop_directory = '/user/test/'
# ftp -> local

# with pysftp.Connection(host=ftpserver, username='ftp_cloudera', password='LGEdBn6u') as sftp:
#     Test the connection to the ftp server works well.
# data = sftp.listdir()
# print data
# print 'Connection gooood!'
#
# for file_to_ftp in data:
#     sftp.get_r(remotedir='/', localdir='/Users/Projects/FTP-connections/'+timeStamp, preserve_mtime=True)
# sftp.getfo('/' + file_to_ftp, open('/Users/Projects/FTP-connections/test/' + file_to_ftp, 'wb'))
# file_name_to_use_in_s3 = "%s/%s" % (timeStamp_s3_folder, str(file_to_ftp))
# k.name = file_name_to_use_in_s3
# k.set_contents_from_filename('/Users/Projects/FTP-connections/test/'+file_to_ftp)

# TODO: may have to refine it. Does the dumb-est thing possible.
local_directory_ftp = current_working_directory + '/' + timeStamp_s3_folder
os.mkdir(local_directory_ftp)
os.chdir(local_directory_ftp)

call(current_working_directory + '/' + "copyfromftp.sh")   # shell script to copy the files to local

# local -> s3

os.chdir(current_working_directory)

local_directory_s3 = local_directory_ftp + extension  # Only selects the files of the extension .txt.

for file_to_s3 in glob.glob(local_directory_s3):
    print file_to_s3
    file_name_to_use_in_s3 = "%s/%s" % (timeStamp_s3_folder, os.path.basename(file_to_s3))
    k.name = file_name_to_use_in_s3
    k.set_contents_from_filename(file_to_s3)

timeStamp = datetime.datetime.now()
print 'Process competed at: ' + str(timeStamp)
last_imported_data.write(str(timeStamp))
last_imported_data.close()

shutil.rmtree(local_directory_ftp)  # Removing the folder completely after the files are in s3.

# s3 -> hadoop

# TODO: correct hdfs location. We need to configure our cluster in a way to use this command.
#
#
#   https://github.com/Aloisius/hadoop-s3a/blob/master/README.md
#
s3_directory = "s3a://location/" + timeStamp_s3_folder + extension

check_call(["hadoop", "distcp", s3_directory, hadoop_directory])


