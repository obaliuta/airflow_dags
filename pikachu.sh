# bash pikachu.sh poke*
grep 'Pika' $1 | cut -d ";" -f 2,5 > pika.csv