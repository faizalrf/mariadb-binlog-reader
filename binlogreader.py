import os
import re
import glob
import sys
import string

#Function to count the number of lines in a file
def FileLength(FileName):
    with open(FileName) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

#Function to return a distinct list removing duplicates
def distinct(aList):
  return list(dict.fromkeys(aList))

#Counts the individual items in the array and generate a summary list
def countElements(aList):
    DistinctItems = distinct(aList)
    Report = []
    for Item in DistinctItems:
        Report.append(Item + "," + str(aList.count(Item)))

    Report.sort()
    return Report

#Function to print an array
def printReport(aList):
    print("\n*********** Start of the Binary Log Report ***************")
    for Line in aList:
        print(Line)

    print("*********** End of the Binary Log Report ***************\n")

#Main Module
def main():
    #Makes sure all three parameters are provided!
    if (len(sys.argv) < 4):
        print ("Please specify the Binary Log Path, Bin Log filename and starting log file name")
        print ("\nExample:")
        print ("shell> python binlogreader.py /var/lib/mysql mariadb-bin 0")
        print ("\nThe above looks at the: \n\t/var/lib/mysql folder for binary logs\n\tmariadb-bin as the binlog file naming convention\n\t0 as the starting binary log file name\n\n")
        return

    DetailedOutFile = "binlog.out"
    SummaryOutFile = "binlogsummary.out"

    try:
        Path = sys.argv[1]
        BinLogFileName = sys.argv[2]
        StartPosition = int(sys.argv[3])
    except:
        Path = "/var/lib/mysql"
        BinLogFileName = "mariadb-bin"
        StartPosition = 0

    FullBinLogPath = Path + '/' + BinLogFileName;

    Files = []

    Files = glob.glob(FullBinLogPath + ".*")
    try:
       Files.remove(FullBinLogPath + ".index")
    except:
        print("Ignoring Binary Log Index File...")

    # Sort the filenames
    Files.sort();

    os.system(">" + DetailedOutFile);

    #Process the files
    print("\n*********** Reading Binary Logs ***********")

    #Prefine the Action Items to search for
    SearchAction = []
    SearchAction.append("CREATE TABLE ")
    SearchAction.append("CREATE INDEX ")
    SearchAction.append("DROP TABLE ")
    SearchAction.append("ALTER TABLE ")
    SearchAction.append("### INSERT ")
    SearchAction.append("### UPDATE ")
    SearchAction.append("### DELETE ")

    for BinLogFile in Files:
        try:
           BinLogNumber = int(os.path.splitext(BinLogFile)[1].replace('.', '0'))
           if (BinLogNumber >= StartPosition):
               print ("Reading " + BinLogFile + "...")
               for Action in SearchAction:
                  BinLogCmd = 'mysqlbinlog -v ' + BinLogFile + ' | grep "' + Action + '" >> ' + DetailedOutFile
                  os.system(BinLogCmd)
           else:
               print("Skipping binlog file " + str(BinLogNumber))
        except:
            print(BinLogFile + " is not a BinLog file...")

    #Distinct the file
    os.system("sort -u " + DetailedOutFile + " > " + SummaryOutFile)
    
    print("*********** Finished Reading Binary Logs ***********")

    print("\n*********** Getting Distinct Changes ***********")
    
    #Open the extracted Binlogs file and start parsing
    try:
        ExtractedBinlog = open(SummaryOutFile, "r")
        LogLines = ExtractedBinlog.readlines()
    except:
        print("Unable to read the extracted binary log file!")

    print("*********** Distinct Changes Identified ***********")
    #Stores various action items
    TransList=[]

    print("\n*********** Generating Report ***********")

    #Parse the Binlog Output
    for LogLine in LogLines:
        #This will remove all white spaces including tabs etc and make the string properly formatted with a single space
        LogLine = " ".join(LogLine.split())

        print("Generating Summary for [" + LogLine + "]")
        with open(DetailedOutFile) as Details:
           Counter = 0
           for DetailLine in Details:
               # Remove inconsistencies in the string, this is a heavy operation
               DetailLine = " ".join(DetailLine.split())
               Counter += 1 if LogLine in DetailLine else 0

           TransList.append(LogLine.replace('### ', '') + "," + str(Counter))

    print("*********** Report Generated ***********")
    
    #Print the formatted report from the binary logs!
    printReport(TransList)

if __name__== "__main__":
  main()
