
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"
items_dat_filename = "items.dat"
user_dat_filename = "user.dat"
bids_dat_filename = "bids.dat"
category_dat_filename = "category.dat"

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

def bidderLocation(bidder):
    if 'Location' in bidder:
        return bidder['Location']
    else:
        return "NULL"

def bidderCountry(bidder):
    if 'Country' in bidder:
        return bidder['Country']
    else:
        return "NULL"

def getBuyPrice(item):
    if 'Buy_Price' in item:
        return fix_string(transformDollar(item['Buy_Price']))
    else:
        return "NULL"

# 1. Escape every instance of a double quote with another double quote.
# 2. Surround all strings with double quotes.
def fix_string(ss):
    ss = ss.replace('"','""')
    ss = '"'+ss+'"'
    return ss

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    with open(json_file, 'r') as f,\
        open(items_dat_filename, 'a') as items_dat,\
        open(user_dat_filename, 'a') as user_dat,\
        open(bids_dat_filename, 'a') as bids_dat,\
        open(category_dat_filename, 'a') as category_dat:

        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
        
        for item in items:
            # the format of a line in items_Dat is 
            # 'ItemID'|'Name'|'Currently'|'BuyPrice'|'NumberOfBids'|'Started'|'Ends'\n
            item_line = "{ItemID}{separator}{Name}{separator}{Currently}{separator}{Buy_Price}{separator}{FirstBid}{separator}{NumberOfBids}{separator}{Started}{separator}{Ends}\n".format\
            (ItemID = item['ItemID'],\
            separator = columnSeparator,\
            Name = fix_string(item['Name']),\
            Currently = fix_string(transformDollar(item['Currently'])),\
            Buy_Price = fix_string(getBuyPrice(item)),\
            FirstBid = fix_string(transformDollar(item['First_Bid'])),\
            NumberOfBids = item['Number_of_Bids'],\
            Started = transformDttm(item['Started']),\
            Ends = transformDttm(item['Ends']))
            items_dat.write(item_line)

            # the format of a line in category.dat is
            # 'Category'|'ItemID'\n
            categories = item['Category']
            for category in categories:
                category_line = "{Category}{separator}{ItemID}\n".format\
                (Category = category,\
                ItemID = item['ItemID'],\
                separator = columnSeparator)
                category_dat.write(category_line)

            # the format of a line in user_dat is
            # 'UserID'|'Location'|'Country'|'Rating'\n
            user_line = "{UserID}{separator}{Location}{separator}{Country}{separator}{Rating}\n".format\
            (UserID = fix_string(item['Seller']['UserID']),\
            separator = columnSeparator,\
            Location = fix_string(item['Location']),\
            Country = fix_string(item['Country']),\
            Rating = item['Seller']['Rating'])
            user_dat.write(user_line)

            bids = item['Bids']
            if bids is None:
                # the format of a line in bids_dat is
                # 'Auction ID'|'Seller id'|'Bidder id'|'Time'|'Amount'
                bid_line = "{ItemID}{separator}{SellerID}{separator}{BidderID}{separator}{Time}{separator}{Amount}\n".format\
                (ItemID = item['ItemID'],\
                SellerID = fix_string(item['Seller']['UserID']),\
                BidderID = "NULL",\
                Time = "NULL",\
                Amount = "NULL",\
                separator = columnSeparator)
                bids_dat.write(bid_line)
            else:
                for bid in bids:
                    # the format of a line in bids_dat is
                    # 'Auction ID'|'Seller id'|'Bidder id'|'Time'|'Amount'\n
                    bid_line = "{ItemID}{separator}{SellerID}{separator}{BidderID}{separator}{Time}{separator}{Amount}\n".format\
                    (ItemID = item['ItemID'],\
                    SellerID = fix_string(item['Seller']['UserID']),\
                    BidderID = fix_string(bid['Bid']['Bidder']['UserID']),\
                    Time = transformDttm(bid['Bid']['Time']),\
                    Amount = fix_string(transformDollar(bid['Bid']['Amount'])),\
                    separator = columnSeparator)
                    bids_dat.write(bid_line)

                    bidder_line = "{UserID}{separator}{Location}{separator}{Country}{separator}{Rating}\n".format\
                    (UserID = fix_string(bid['Bid']['Bidder']['UserID']),\
                    separator = columnSeparator,\
                    Rating = bid['Bid']['Bidder']['Rating'],\
                    Location = fix_string(bidderLocation(bid['Bid']['Bidder'])),\
                    Country = fix_string(bidderCountry(bid['Bid']['Bidder'])))
                    user_dat.write(bidder_line)
            pass

"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print ("Success parsing " + f)

if __name__ == '__main__':
    main(sys.argv)
