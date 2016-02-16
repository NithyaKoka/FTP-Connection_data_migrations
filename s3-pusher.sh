#!/usr/bin/env bash
#!/bin/bash -e

# 2010-09-19 Marc Limotte

# Run continuously (every 30 minutes) as a cron.
#
# Looks for directories in HDFS matching a certain pattern and moves them to S3, using Amazon's new
# distcp replacement, S3DistCp.
#
# It creates marker files (_directory_.done and _directory_.processing) at the S3 destination, so
# that it can synchronize when multiple instances of the script are running, and so that
# down-stream processes will know when the data directory is ready for consumption.  This wouldn't
# be necessary if we were moving a single file, since it wouldn't show up at the destination until
# it was complete.  But is necessary when moving directories of files, where some files might be
# completely transferred, but not all.


###########
# Install #
###########

# 1) Configure the local hadoop cluster. Add to $HADOOP/conf/core-site.xml:
#  <property>
#    <name>fs.s3.awsAccessKeyId</name>
#    <value>_____add id______</value>
#  </property>
#
#  <property>
#    <name>fs.s3.awsSecretAccessKey</name>
#    <value>______add secret key_____________</value>
#  </property>

# 2) Install Amazon's S3DistCp (amazon has their own version of a distcp-like
# utility, it includes better error handling and performance improvements.
#   wget http://richcole-publish.s3.amazonaws.com/libs/S3DistCp/1.0-002/S3DistCp-1.0.tar.gz
#   sudo tar xf S3DistCp-1.0.tar.gz -C /usr/local/
# Can be installed anywhere, just change the DISTCP_JAR variable below to match

# 3) Make sure s3cmd is installed and configured

#############
# Variables #
#############

# Paths
HADOOP=/usr/bin/hadoop
S3CMD=/usr/bin/s3cmd
DISTCP_JAR=/usr/local/S3DistCp-1.0/S3DistCp.jar
DISTCP_MAIN=com.amazon.elasticmapreduce.s3distcp.S3DistCp

# source
# Given the values below, the script will look for directories that match:
#   hdfs:///user/creator/dt=*
# Notice that no hostname is specified (triple / after hdfs:), so it will use the local
# hadoop conf to find HDFS.
DFS_ROOT=/user/creator
PATTERN="dt="

# dest
BUCKET=my-bucket
S3_PREFIX=/data/in
# Use the S3 path with s3cmd
S3_DEST="s3://$BUCKET$S3_PREFIX"
# Use the S3N path with the S3DistCp. This utility should understand S3, but there is a big that
# adds an extra "/" into the destination path. Using S3N is a work-around.  Also note, according
# to Amazon's documentation s3: and s3n: are native, and s3bf: is a block file system.
S3N_DEST="s3n://$BUCKET$S3_PREFIX"


#############
# Functions #
#############

function s3touch {
  file=$1
  touch /tmp/$file
  $S3CMD put /tmp/$file $S3_DEST/$file
}

function s3rm {
  file=$1
  $S3CMD del $S3_DEST/$file
}

function upload {
  INPUT_DIR=$1
  OUTPUT_DIR=$2
  $HADOOP jar $DISTCP_JAR $DISTCP_MAIN $INPUT_DIR $OUTPUT_DIR
}

function hdfsDeleteOlderThan {
  root=$1
  pattern=$2
  daysAgo=$3
  distcpdirs=$( $HADOOP dfs -stat %n $root/* | egrep "$pattern" ) || true
  for base in $distcpdirs; do
    path=$root/$base
    stamp=$( expr `$HADOOP dfs -stat "%Y" $path` / 1000 )
    age_days=$(expr \( `date +%s` - $stamp \) / 86400) || true
    if [ $age_days -gt $daysAgo ]; then
      $HADOOP dfs -rmr $path
    fi
  done
}


################
# Main Process #
################

### Upload data directories to S3

datadirs=$( $HADOOP dfs -stat %n $DFS_ROOT/* | egrep "$PATTERN" ) || true
for base in $datadirs ; do
  d=$DFS_ROOT/$base

  echo found $d in HDFS

  # Data directory is ignored, unless it contains a .done file
  # Comment out this check if you don't want/use this behavior
  echo " check if data directory is complete"
  if [ `$HADOOP dfs -ls $d/.done | wc -l` -eq 0 ]; then

      echo " not complete, skipping."

  else

      echo " checking against S3"

      if [ `$S3CMD ls $S3_DEST/$base.processing | wc -l` -eq 0 ] \
         && [ `$S3CMD ls $S3_DEST/$base.done | wc -l` -eq 0 ]; then

        # TODO small chance for race condition here. should add something to fix this,
        #      but it's not a big problem for us.
        echo " not in S3, continuing with upload"
        s3touch $base.processing

        # exit status of upload will be non-zero in the event of an error, causing this
        # script to exit.
        upload hdfs://$d/ $S3N_DEST/$base/

        s3touch $base.done
        s3rm $base.processing
        echo " upload to S3 complete, removing from HDFS."

	# If you want to remove the source files in HDFS, uncomment:
        #$HADOOP dfs -rmr $d
	# Alternatively, you could delete these source directories after some number of
        # days (e.g. 10):
        #hdfsDeleteOlderThan $DFS_ROOT $PATTERN 10

      else

        echo " found .processing or .done in S3, skipping."

      fi

  fi

done


### Complete

echo Completed at `/bin/date --utc`