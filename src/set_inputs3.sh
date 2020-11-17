input_list="a b c d e f g h i j k l m"
num="0 1 2"
s3_bucket="s3://jg-marla-input1"
path="../assets/"
for file in $input_list; do
  for n in $num; do
    aws s3 cp $path'part'$n'a'$file $s3_bucket
  done
done
