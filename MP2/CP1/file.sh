cat [t]*.log > trans_merge.log &&
sed '/Connect/d' trans_merge.log > trans_merge_new.log &&
rm trans_merge.log &&
sort -k 2 -t : trans_merge_new.log > sorted_trans.log
