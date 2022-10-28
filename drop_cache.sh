num=3
# 0: do not release (the default value)
# 1: release page cache
# 2: release dentries and inodes
# 3: release all caches 
free -h
echo $num > /proc/sys/vm/drop_caches
echo -e "after dropping cache"
free -h