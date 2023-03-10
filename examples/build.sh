#!/bin/bash
# file-related parameters
filename=100G
inputdir=/mnt/disk12/
outputdir=/mnt/disk12/
newfilename=new-64Mx16
#data shards
k=4
#parity shards
m=2
#used disk number
dn=10
#block size
bs=67108864
#memory limit
mem=32
#failed disk number
fn=1
#specified failed disk, starting from 0, 
# use comma to split
fd=0

#slowNumber
sn=4
#slowLatency
sl=4
#conStripe
cs=1
# 4k 4096
# 1M 1048576
# 4M 4194304
# 16M 16777216
# 32M 33554432
# 64M 67108864
# 128M 134217728
# 256M 268435456
LOG_FILE="test.log"

go build -o main ./main.go
now=`date +%c` 
echo -e "sh: The program started at $now." >> LOG_FILE
#------------------------encode a file--------------------------
mode="recover"
if [ $mode == "recover" ]; then
    #---------------------------repair the file----------------------
    # recover a file
    methods1=("FIRST_K" "RANDOM_K" "LB_HDR")
    methods2=("SEQ" "SS_HDR")
    # methods=("fsr" "SEQ" "ss-hdr")
    #to avoid serendipity, we shuffle the order of methods
    #for `RAND_TIME` time(s)
    RAND_TIME=1
    for ((i=0;i<$RAND_TIME;i++));do
        echo -e "\n\nsh: experiment $i" >> LOG_FILE
        # shuffled_methods=(`shuf -e ${methods[@]}`)
        for method1 in ${methods1[@]};do
            for method2 in ${methods2[@]};do
                # drop the cache to make the result more convincing
                ../drop_cache.sh
                echo -e "\nmethod1:$method1 method2:$method2" >> LOG_FILE
                start=`date +%s%N`
                ./main -md hybrid -md1 $method1 -md2 $method2 -fmd diskFail -fd $fd -f $inputdir$filename -o -readbw -q -sl $sl -sn $sn -cs $cs
                end=`date +%s%N`
                cost=`echo $start $end | awk '{ printf("%.3f", ($2-$1)/1000000000) }'`
                echo -e "sh: previous procedure consumed $cost s" >> LOG_FILE
            done
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
