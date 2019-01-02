#!/bin/bash

gen_column()
{
    num_of_items=$1
    num_of_keys=$((2 ** $2))
    cur_file=$3"_cols/""col"$(($2/4-1))".dat"

    curCol=($2)
    for (( i=1; i<=$num_of_items; i++))
    do
        cur_val=$(($RANDOM%$num_of_keys))
        curCol+=( $cur_val )
    done

    printf "%s\n" "${curCol[@]}" > $cur_file
}

if [ $# -lt 2 ]; then
    echo "Missing arguments, sample usage: ./datagen filename #_of_values"
    exit
else
   file_name=$1
   data_size=$2
   printf "%s\n2" "$data_size" > $file_name
fi


for bit_size in 4 8; do
    gen_column $data_size $bit_size $file_name
done
