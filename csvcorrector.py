import glob, os
import sys
import csv
dirToVideos = sys.argv[1]
os.chdir(dirToVideos)
if not os.path.exists(os.path.dirname('altered/')):
    os.makedirs(os.path.dirname('altered/'))

def find_nth(s, x, n):
    i = -1
    for _ in range(n):
        i = s.find(x, i + len(x))
        if i == -1:
            break
    return i

for file in glob.glob("*.csv"):
    with open(file,'r') as csvinput:
        with open("altered/"+file, 'w+') as csvoutput:
            writer = csv.writer(csvoutput, lineterminator='\n')
            reader = csv.reader(csvinput)
            all = []
            if(file.count('_')>4):
                n = find_nth(file,'_',2)
                first_part = file[:n]
                last_part = file[n+1:]
                file = first_part + last_part
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
