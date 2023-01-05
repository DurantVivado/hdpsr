# 0: do not release (the default value)
# 1: release page cache
# 2: release dentries and inodes
# 3: release all caches 
free -h # cat /proc/meminfo
echo 1 > /proc/sys/vm/drop_caches
echo 2 > /proc/sys/vm/drop_caches
echo 3 > /proc/sys/vm/drop_caches
sleep 5
echo -e "dropped all cache"
free -h # cat /proc/meminfo