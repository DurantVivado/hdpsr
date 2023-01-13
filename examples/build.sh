#!/bin/bash
# file-related parameters
filename=test-64Mx160
inputdir=/mnt/disk16/
outputdir=/mnt/disk15/
newfilename=new-64Mx16
#data shards
k=4
#parity shards
m=2
#used disk number
dn=14
#block size
bs=67108864
#memory limit
mem=2
#failed disk number
fn=1
#specified failed disk, starting from 0, 
# use comma to split
fd=0

#slowNumber
sn=4
#slowLatency
sl=4
# 4k 4096
# 1M 1048576
# 4M 4194304
# 16M 16777216
# 32M 33554432
# 64M 67108864
# 128M 134217728
# 256M 268435456


go build -o main ./main.go
now=`date +%c` 
echo -e "sh: The program started at $now."  
#------------------------encode a file--------------------------
mode="recover"
if [ $mode == "recover" ]; then
     
    #---------------------------repair the file----------------------
    # recover a file
    # methods=("fsr" "fsr-so_c" "fsr-so_g")
    # methods=("fsr" "fsr-b_1K" "fsr-b_FK" "fsr-b_R" "fsr-b_B")
    # methods=("fsr" "mpsrap" "mpsras" "mpsrpa")
    methods=("fsr" "fsr-old")

    #to avoid serendipity, we shuffle the order of methods
    #for `RAND_TIME` time(s)
    RAND_TIME=10
    for ((i=0;i<$RAND_TIME;i++));do
        echo -e "\n\nsh: experiment $i"
        shuffled_methods=(`shuf -e ${methods[@]}`)
        for method in ${shuffled_methods[@]};do
            # drop the cache to make the result more convincing
            ../drop_cache.sh
            echo -e "method:$method" 
            start=`date +%s%N`
            ./main -md $method -fmd diskFail -fd $fd -f $inputdir$filename -o -readbw -q -sl $sl -sn $sn
            end=`date +%s%N`
            cost=`echo $start $end | awk '{ printf("%.3f", ($2-$1)/1000000000) }'`
            echo -e "sh: previous procedure consumed $cost s"
        done
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
