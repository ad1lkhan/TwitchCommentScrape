import glob, os
import sys
import csv
dirToVideos = sys.argv[1]
os.chdir(dirToVideos)
if not os.path.exists(os.path.dirname('altered/')):
    os.makedirs(os.path.dirname('altered/'))

for file in glob.glob("*.csv"):
    with open(file,'r') as csvinput:
        with open("altered/"+file, 'w+') as csvoutput:
            writer = csv.writer(csvoutput, lineterminator='\n')
            reader = csv.reader(csvinput)
            all = []
            vid,name,game,comments=file.split("_")
            row = next(reader)
            row.append('name')
            row.append('game')
            all.append(row)

            for row in reader:
                row.append(name)
                row.append(game)
                all.append(row)

            writer.writerows(all)
            print("Completed: "+vid)
