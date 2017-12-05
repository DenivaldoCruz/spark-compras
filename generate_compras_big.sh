#!/bin/bash
max=50
for i in `seq 2 $max`
do
    cat compras.txt >> compras_big.txt
done
