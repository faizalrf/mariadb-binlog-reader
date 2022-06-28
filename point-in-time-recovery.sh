#!/bin/bash

# Global variables 
# cat xtrabackup_binlog_info | awk -F"-" '{print $NF}'
# cat xtrabackup_binlog_info | grep -o "mariadb-bin.[0-9]*" | awk -F"mariadb-bin." '{print $NF}'
binlogFile=""
tmpFile=/tmp/extract.log
gtidSummary=/tmp/GTID_summary.out
gtidPosition=/tmp/GTID_position.out
rm -f ${gtidSummary}
binlogWithGTID=""
nextBinLogIsTheOne=0
restoreScript=/tmp/restore.sh
echo "# Restore Script" > ${restoreScript}
chmod 777 ${restoreScript}
commandString="mariadb-binlog "

ListGTID() {
   # Scan through all the files in the list
   fileNumber=${FirstFile}
   for file in ${FILES}
   do
      binlogFile=${BinLogPath}${file}
      # If Confirmed that next binary log is the one to be scanned from start then break this loop and continue
      echo "GRID to search ${GTID_To_Search}"
      if [ ${nextBinLogIsTheOne} -gt 0 ]; then
         # Generate new Sequence and contunue 
         FirstFile=${fileNumber}
         FILES=$(ls -nrt ${BinLogPath}${BinLogName}.*[0-9]* | awk -F '.' '{if ($2 >= '"$FirstFile"') print $0}' | awk -F '.' '{if ($2 <= '"$LastFile"') print $0}' | grep -o "${BinLogName}.*[0-9]*")
         break
      fi
      if [ ! -f ${binlogFile} ]; then
         echo "${binlogFile} file not found!"
         continue
      fi
      echo "Reading ${binlogFile}"

      mariadb-binlog ${binlogFile} | grep -n "GTID *[0-9]*-[0-9]*-[0-9]*" | grep "${GTID_To_Search}" -A 1 > ${gtidSummary}
      ret=$?
      # If any problem reading the GTID summary, exit 1
      if [[ ${ret} == 0 ]]; then
         cat ${gtidSummary}
         SummaryLines=$(cat ${gtidSummary} | wc -l)
         if [ ${SummaryLines} -eq 1 ]; then
            nextBinLogIsTheOne=1
            echo "This is the last GTID in ${file}, next binlog will be applied to the DB from very first transaction..."
         elif [ ${SummaryLines} -eq 2 ]; then
            nextBinLogIsTheOne=2
            GTID_To_Search=$(tail -1  ${gtidSummary} | grep -o "?*[0-9]*-[0-9]*-[0-9]*")
            binlogWithGTID=${file}
            echo "The ${file} contains the GTID ${GTID_To_Search}, this file will be applied partially from the next GTID position..."
            continue
            # Contunue here because we have to scan the same file in the next function 
         fi
      fi
      ((fileNumber++))
   done
}

FindGTIDPosition() {
   for file in ${FILES}
   do
      binlogFile=${BinLogPath}${file}
      if [[ ${nextBinLogIsTheOne} == 2 ]]; then
         if [[ ${binlogWithGTID} == ${file} ]]; then
            mariadb-binlog ${binlogFile} | grep "GTID ${GTID_To_Search}" -B 1 > ${gtidPosition}
            startPos=$(grep "# at " ${gtidPosition} | head -1 | grep -o -E '[0-9]+')
            ret=$?
            if [[ ${ret} == 0 ]]; then
               echo "Start position ${startPos}"
               echo "mariadb-binlog ${binlogFile} --start-position=${startPos} | mariadb" >> ${restoreScript}
               continue
            else
               echo "Error reading GTID position..."
               echo
               exit 1
            fi
         fi
      fi
      echo "mariadb-binlog ${binlogFile} | mariadb" >> ${restoreScript}
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

GenerateFileList() {
   if [[ ${FirstFile} > "0" ]]; then
      echo "First File ${FirstFile}"
   elif [ -d ${TargetDir} ]; then
      if [ -f ${TargetDir}/xtrabackup_binlog_info ]; then
         #Find the filenumber from the binlog file name in the xtrabackup_binlog_info
         FirstFile=$(cat ${TargetDir}/xtrabackup_binlog_info | grep -o "${BinLogName}.[0-9]*" | awk -F"${BinLogName}." '{print $NF}')
         FirstFile=$(expr ${FirstFile} + 0)
         echo "First File ${FirstFile}"

         GTID_To_Search=$(cat ${TargetDir}/xtrabackup_binlog_info | awk '{print $3}')
      else
         echo "Invalid backup directory path, `xtrabackup_binlog_info` file not found..."
         exit 1
      fi
   fi
   if [[ ${LastFile} > "0" ]]; then
      echo "Last File ${LastFile}"
      if [[ ${LastFile} < ${firstFile} ]]; then
         echo "--end-file=${LastFile} must be higher than --start-file=${firstFile}, please check the arguments..."
         echo "You may also want to validate the xtrabackup_binlog_info file for starting file position"
         echo
         exit 1
      fi

   else
      LastFile=$(ls -nrt ${BinLogPath}${BinLogName}.[0-9]* | awk {'print $9'} | tail -1 | awk -F"${BinLogName}." '{print $NF}')
      LastFile=$(expr ${LastFile} + 0)
   fi

   FILES=$(ls -nrt ${BinLogPath}${BinLogName}.*[0-9]* | awk -F '.' '{if ($2 >= '"$FirstFile"') print $0}' | awk -F '.' '{if ($2 <= '"$LastFile"') print $0}' | grep -o "${BinLogName}.*[0-9]*")
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

if [[ ${TargetDir: -1} != "/" ]]; then
   TargetDir="${TargetDir}/"      
fi

if [ ! -d ${TargetDir} ]; then
   echo "Invalid MariaBackup path containing `xtrabackup_binlog_info` file..."
   exit 1
fi

GenerateFileList

ListGTID

FindGTIDPosition

exit 0
