#!/bin/bash

backup() {
   echo "GTID to Search = ${GTID_To_Search}"
   FILES=$(seq -f "${BinLogName}.%06g" ${FirstFile} ${LastFile})
   tmpFile=/tmp/extract.log
   gtidSummary=/tmp/GTID_summary.out
   rm -f ${gtidSummary}
   #nextBinLogIsTheOne=0
   touch /tmp/point-in-time-restore.sh
   chmod 500 /tmp/point-in-time-restore.sh
   commandString="mariadb-binlog "

   if [ ! -d ${BinLogPath} ]; then
      echo "Invalid binlog path ${BinLogPath} !"
      exit 0
   fi

   if [[ ${BinLogPath} == "" ]]; then
      BinLogPath="./"
   fi

   if [[ ${BinLogPath: -1} != "/" ]]; then
      BinLogPath="${BinLogPath}/"      
   fi
   
   # Scan through all the files in the list
   for file in ${FILES}
   do
      binlogFile=${BinLogPath}${file}
      if [ ! -f ${binlogFile} ]; then
         echo "${binlogFile} file not found!"
         continue
      fi

      if [ ${nextBinLogIsTheOne} -eq 1 ]; then
         commandString="${commandString} ${binlogFile}"
         echo ${commandString} >> point-in-time-restore.sh
         continue
      fi

      echo "Reading ${binlogFile}..."

      readBinlog=${binlogFile}.tmp
      mariadb-binlog ${binlogFile} > ${readBinlog}
      ret=$?
      if [[ ${ret} == 0 ]]; then
         # Extract the transaction Position from the binary log
         # TODO extract next GTID position as well for mariabackup to start restore
         grep -n "GTID *[0-9]*-[0-9]*-[0-9]*" ${readBinlog} | grep "GTID ${GTID_To_Search}" -A 1 > ${gtidSummary}

         # Stop searching any other binlog files and conclude the next file is the one
         if [ $(cat ${gtidSummary} | wc -l) -eq 1 ]; then
            nextBinLogIsTheOne=1
            continue
         else
            GTID_To_Search=$(tail -1  ${gtidSummary} | grep -o "?*[0-9]*-[0-9]*-[0-9]*")
         fi
 
         startPos=$(grep "GTID ${GTID_To_Search}" -B 10 ${readBinlog} | grep "# at " | tail -1 | grep -o -E '[0-9]+')
         ret=$?
         if [[ ${ret} == 0 ]]; then
            commandString="mariadb-binlog ${binlogFile} --start-position=${startPos} | mariadb"
            echo ${commandString} >> point-in-time-restore.sh
            nextBinLogIsTheOne=1
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

ListGTID() {
   # Scan through all the files in the list
   fileNumber=${FirstFile}
   for file in ${FILES}
   do
      binlogFile=${BinLogPath}${file}
      # If Confirmed that next binary log is the one to be scanned from start then break this loop and continue

      if [ ${nextBinLogIsTheOne} -gt 0 ]; then
         # Generate new Sequence and contunue 
         FirstFile=${fileNumber}
         FILES=$(seq -f "${BinLogName}.%06g" ${FirstFile} ${LastFile})
         break
      fi
      if [ ! -f ${binlogFile} ]; then
         echo "${binlogFile} file not found!"
         continue
      fi
      echo "Reading ${binlogFile}..."

      mariadb-binlog ${binlogFile} | grep -n "GTID *[0-9]*-[0-9]*-[0-9]*" | grep "${GTID_To_Search}" -A 1 > ${gtidSummary}
      ret=$?
      # If any problem reading the GTID summary, exit 1
      if [[ ${ret} == 0 ]]; then
         SummaryLines=$(cat ${gtidSummary} | wc -l)
         if [ ${SummaryLines} -eq 1 ]; then
            nextBinLogIsTheOne=1
         elif [ ${SummaryLines} -eq 2 ]; then
            nextBinLogIsTheOne=2
            GTID_To_Search=$(tail -1  ${gtidSummary} | grep -o "?*[0-9]*-[0-9]*-[0-9]*")
            continue
            # Contunue here because we have to scan the same file in the next function 
         fi
      fi
      ((fileNumber++))
   done
}

GenerateRestoreScript() {
   echo "GTID to search ${GTID_To_Search}"
   if [ ${nextBinLogIsTheOne} -eq 1 ]; then
      echo "Somthing!"
   fi

   for file in ${FILES}
   do
      # Find the Position of this new GTID
      echo "Next Processing ${file}"
   done
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
   "--target-dir")
     TargetDir=${VALUE};;
   "--datadir")
     DataDir=${VALUE};;
   "--restore-mode")
     LastFile=${VALUE};;
   *)
     echo "Invalid arguments...";;
   esac
done

FILES=$(seq -f "${BinLogName}.%06g" ${FirstFile} ${LastFile})
binlogFile=""
tmpFile=/tmp/extract.log
gtidSummary=/tmp/GTID_summary.out
rm -f ${gtidSummary}
nextBinLogIsTheOne=0
touch /tmp/point-in-time-restore.sh
chmod 500 /tmp/point-in-time-restore.sh
commandString="mariadb-binlog "

if [ ! -d ${BinLogPath} ]; then
   echo "Invalid binlog path ${BinLogPath} !"
   exit 0
fi

if [[ ${BinLogPath} == "" ]]; then
   BinLogPath="./"
fi

if [[ ${BinLogPath: -1} != "/" ]]; then
   BinLogPath="${BinLogPath}/"      
fi

ListGTID

if [ ! -f ${gtidSummary} ]; then
   echo "Error processing, unable to open the GTID Summary ${gtidSummary}"
fi

GenerateRestoreScript

exit 0
