# unzip search log
#!/bin/bash

for f in ../data/raw/*_search.log.tar.gz
do
 echo "Processing $f"
 # awk programming language, -F defines seperator '{print $1}' takes element from position 1
 d=$(echo ${f} | awk -F/ '{print $4}')
 fn=$(echo ${d}| awk -F. '{print $1}')
 echo "rename file as ${fn}"
 
 tar -xvzf  $f 
 mv *_search.log  ../data/search/${fn}.log
done


# append file_name to each row (will be used for date)
cd ../data/search/
for f in *.log
do
 echo "Processing $f"
 awk -v var="$f" '{print $0,"\t",var}' $f > ${f}.fn
done

# cat all log with filename to one file
cat ../data/search/*.log.fn > ../data/all_search.log.fn

# counter=1
# for f in ../data/raw/*_search.log.tar.gz
# do
# tar -xvzf $f -C ../data/raw/result
# for g in ../data/raw/result/*
# do
# mv $g "../data/raw/result/final/"$counter"_search.log"
# done
# counter=$((counter+1))
# done


# # # cat all log with filename to one file
# cat ../data/raw/result/final/*.log > ../data/raw/result/final/all_search.log