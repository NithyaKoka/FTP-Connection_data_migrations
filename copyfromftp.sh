#!/usr/bin/env bash

export SSHPASS=sshpassowrd
sshpass -e sftp -oBatchMode=no -b - ftp_user@example.ftp.com << !
   cd /
   mget ./*.txt
   bye
!
