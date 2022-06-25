#!/bin/bash

searchGTID() {
   echo "GTID to Search = ${GTID_To_Search}"
   FILES=$(seq -f "${BinLogName}.%06g" ${FirstFile} ${LastFile})
   tmpFile=/tmp/extract.log

   if [ ! -d ${BinLogPath} ]; then
      echo "Invalid binlog path ${BinLogPath} !"
      exit 0
   fi

   if [[ ${BinLogPath} == "" ]]; then
      BinLogPath="./"
   fi

   # Scan through all the files in the list
   for file in ${FILES}
   do
      binlogFile=${BinLogPath}${file}
      if [ ! -f ${binlogFile} ]; then
         echo "${binlogFile} file not found!"
         continue
      fi
      echo "Reading ${binlogFile}..."

      readBinlog=${binlogFile}.tmp
      mariadb-binlog ${binlogFile} > ${readBinlog}
      ret=$?
      if [[ ${ret} == 0 ]]; then
         # Extract the transaction Position from the binary log
         # TODO extract next GTID position as well for mariabackup to start restore
         startPos=$(grep ${GTID_To_Search} -B 10 -A  ${readBinlog} | grep "# at " | tail -1 | grep -o -E '[0-9]+')
         ret=$?
         if [[ ${ret} == 0 ]]; then
            commandString="mariadb-binlog --start-position=${startPos} | mariadb"
            echo ${commandString} > point-in-time-restore.sh
            break
         fi
         startPos=0
         rm -f ${readBinlog}
      fi
   done
   rm -f ${readBinlog}

   if [[ ${startPos} == 0 ]]; then 
      echo "GTID ${GTID_To_Search} not found in the Binary Logs provided!"
      exit 0
   fi
   echo "Binlog Startinbg position is ${startPos} and for the ${GTID_To_Search} within ${binlogFile}"
}

HomePath=$(pwd)
WorkingPath=${HomePath}/tmp

for ARGS in "$@"
do
   KEY=$(echo ${ARGS} | cut -f1 -d=)

   KEY_LENGTH=${#KEY}
   VALUE="${ARGS:$KEY_LENGTH+1}"
   
   case ${KEY} in
   "--binlog-name")
     BinLogName=${VALUE};;
   "--binlog-path")
     BinLogPath=${VALUE};;
   "--find-gtid")
     GTID_To_Search=${VALUE};;
   "--gtid-start")
     GTID_Start=${VALUE};;
   "--gtid-end")
     GTID_End=${VALUE};;
   "--start-file")
     FirstFile=${VALUE};;
   "--end-file")
     LastFile=${VALUE};;
   *)
     echo "Invalid arguments...";;
   esac
done

# use here your expected variables

searchGTID
exit 0
