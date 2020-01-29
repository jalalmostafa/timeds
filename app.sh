#! /bin/sh

for (( ; ; ))
do
    python timeds.py 2>&1 | tee logs_$(date).log
    sleep $SLEEP_TIME
done
