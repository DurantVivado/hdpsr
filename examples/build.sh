filename=test-64Mx320
inputdir=/mnt/disk15/
outputdir=/mnt/disk16/
#data shards
k=6
#parity shards
m=2
#used disk number
dn=12
#block size
bs=67108864
#memory limit
mem=2
#failed disk number
fn=1
#specified failed disk, starting from 0, 
# use comma to split
fd=1,2
#slow latency of a disk, it's a human-defined latency for
#specific slow disks
sl=4
#slow number of a disk
sn=4
# intra stripe
is=2
# 4k 4096
# 1M 1048576
# 4M 4194304
# 16M 16777216
# 64M 67108864
# 128M 134217728
# 256M 268435456

go build -o main ./main.go

# ./main -md init -k $k -m $m -dn $dn -bs $bs -mem $mem -sn $sn
# ./main -md encode -f $inputdir/$filename -conStripes 100 -o
./main -md fsr -fmd diskFail -fd $fd -f $inputdir$filename -sl $sl -o
# ./main -md psras -fmd diskFail -fn 1 -f $inputdir$filename -sl $sl
# ./main -md psrpa -fmd diskFail -fn 1 -f $inputdir$filename -sl $sl

# to read a file
# ./main -md read -f $filename -conStripes 100 -sp ../../output/$filename

# srchash=(`sha256sum $inputdir$filename|tr ' ' ' '`)
# dsthash=(`sha256sum $outputdir$filename|tr ' ' ' '`)
# echo $srchash
# echo $dsthash
# if [ $srchash == $dsthash ];then 
#     echo "hash check succeeds"
# else
#     echo "hash check fails"
# fi
