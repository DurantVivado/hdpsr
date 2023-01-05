devs=(`ls /sys/block`)
for dev in ${devs[@]}; do
    # echo 10000 > /sys/block/$dev/queue/hang_threshold
    echo $dev
    # cat /sys/block/$dev/queue/hang_threshold
    # to query the number of IO hang caused by disks
    echo "number of hang:"
    cat /sys/block/$dev/hang
    # to acquire the detailed information of IO hang
    cat /sys/kernel/debug/block/$dev/rq_hang
done

