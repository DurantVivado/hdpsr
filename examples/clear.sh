#!/bin/bash
# file-related parameters
for ((i=1;i<=16;i++));do
    echo "rm -rf /mnt/disk$i/*"
    rm -rf /mnt/disk$i/*
done