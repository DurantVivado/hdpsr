#!/bin/bash
# file-related parameters
filename=test-64Mx320
inputdir=/mnt/disk15/
outputdir=/mnt/disk16/
newfilename=new-64Mx16
#data shards
k=6
#parity shards
m=2
#used disk number
dn=12
#block size
bs=1048576
#memory limit
mem=8
#failed disk number
fn=1
#specified failed disk, starting from 0, 
# use comma to split
fd=0
# 4k 4096
# 1M 1048576
# 4M 4194304
# 16M 16777216
# 32M 33554432
# 64M 67108864
# 128M 134217728
# 256M 268435456

# drop the cache to make the result more convincing
../drop_cache.sh 
go build -o main ./main.go
now=`date +%c` 
echo -e "sh: The program started at $now."  
#------------------------encode a file--------------------------
mode="encode"
if [ $mode == "recover" ]; then

#---------------------------repair the file----------------------
    # recover a file
    # methods=("fsr" "fsr-so_c" "fsr-so_g")
    methods=("fsr-b_1K" "fsr-b_FK" "fsr-b_R" "fsr-b_B")
    for method in ${methods[@]};do
        echo -e "method:$method" 
        start=`date +%s%N`
        ./main -md $method -fmd diskFail -fd $fd -f $inputdir$filename -o -readbw -q
        end=`date +%s%N`
        cost=`echo $start $end | awk '{ printf("%.3f", ($2-$1)/1000000000) }'`
        echo -e "sh: previous procedure consumed $cost s"
    done
else
    # init the system
    ./main -md init -k $k -m $m -dn $dn -bs $bs -mem $mem -readbw

    # to encode a file 
    ./main -md encode -f $inputdir$filename -conStripes 100 -o


    # to update a file
    # ./main -md update -f $filename -nf $newfilename
    # to read a file
    ./main -md read -f $filename -conStripes 100 -sp $outputdir$filename -o
    # to remove a file
    # ./main -md delete -f $filename

    # srchash="6cb118a8f8b3c19385874297e291dcbcdf3a9837ba1ca7b00ace2491adbff551"
    srchash=(`sha256sum $inputdir$filename|tr ' ' ' '`)
    dsthash=(`sha256sum $outputdir$filename|tr ' ' ' '`)
    echo -e "source file hash: $srchash"
    echo -e "target file hash: $dsthash"
    if [ $srchash == $dsthash ];then 
        echo -e "hash check succeeds"
    else
        echo -e "hash check fails"
    fi
fi
