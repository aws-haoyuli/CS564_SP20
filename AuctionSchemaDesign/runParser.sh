rm user.dat
rm items.dat
rm bids.dat
rm category.dat
python JSON_parser.py ebay_data/*

sort -o sorted_user.dat user.dat
sort -o sorted_items.dat items.dat
sort -o sorted_bids.dat bids.dat
sort -o sorted_category.dat category.dat

uniq sorted_user.dat > user.dat
uniq sorted_items.dat > items.dat
uniq sorted_bids.dat > bids.dat
uniq sorted_category.dat > category.dat

rm sorted_user.dat
rm sorted_items.dat
rm sorted_bids.dat
rm sorted_category.dat

rm ebay.db
sqlite3 ebay.db ".quit"
sqlite3 ebay.db < create.sql
sqlite3 ebay.db < load.txt