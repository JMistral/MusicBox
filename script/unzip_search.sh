# unzip search log
counter=1
for f in ../data/raw/*_search.log.tar.gz
#for f in /Users/jessie/Desktop/MusicBox/data/raw/result/*
do

 #echo Processing "$f"
 tar -xvzf $f -C ../data/raw/result
for g in ../data/raw/result/*
do
mv $g "../data/raw/result/final/"$counter"_search.log"
done
 #mv $f "/Users/jessie/Desktop/MusicBox/data/raw/result/"$counter"_search.log"
 #mv $f $counter _search.log
  counter=$((counter+1))
 #rename $f > $counter.$f
 #gunzip -c "$f" > "$counter.$f"
done

# mv *_search.log ../data/search/

# cp ../data/raw/*_search.log.gz ../data/search/ 
# gunzip ../data/search/*.gz

# # append file_name to each row (will be used for date)
# cd ../data/search/
# for f in *.log
# do
#  echo "Processing $f"
#  awk -v var="$f" '{print $0,"\t",var}' $f > ${f}.fn
# done

# # cat all log with filename to one file
 cat ../data/raw/result/final/*.log > ../data/raw/result/final/all_search.log