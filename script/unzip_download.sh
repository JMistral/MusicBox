#!/bin/bash
#chmod u+x testscript.sh

for f in ../data/raw/*_down.log.tar.gz
do
 echo "Processing $f"
 # awk programming language, -F defines seperator '{print $1}' takes element from position 1
 d=$(echo ${f} | awk -F/ '{print $4}')
 fn=$(echo ${d}| awk -F. '{print $1}')
 echo "rename file as ${fn}"
 
 tar -xvzf  $f 
 mv *_down.log  ../data/down/${fn}.log
done


# append file_name to each row (will be used for date)
cd ../data/down/
for f in *.log
do
 echo "Processing $f"
 awk -v var="$f" '{print $0,"\t",var}' $f > ${f}.fn
done

# cat all log with filename to one file
cat ../data/down/*.log.fn > ../data/all_down.log.fn