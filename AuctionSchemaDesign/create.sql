drop table if exists Item;
drop table if exists User;
drop table if exists Bid;
drop table if exists Category;

create table Item(
    ItemID INTEGER PRIMARY KEY,
    Name TEXT,
    Currently TEXT,
    BuyPrice TEXT,
    FirstBid TEXT,
    NumberOfBids INTEGER,
    Started TEXT,
    Ends TEXT
);
create table User( 
    UserID TEXT PRIMARY KEY,
    Location TEXT,
    Country TEXT,
    Rating REAL
);
create table Category( 
    Category TEXT,
    ItemID INTEGER,
    
    PRIMARY KEY (Category, ItemID),
    FOREIGN KEY (ItemId) 
      REFERENCES Item (ItemId) 
         ON DELETE CASCADE 
         ON UPDATE CASCADE
);
create table Bid(
    ItemId INTEGER,
    SellerId TEXT,
    BidderId TEXT,
    Time TEXT,
    Amount TEXT,
    
    FOREIGN KEY (SellerId) 
      REFERENCES User (UserId) 
         ON DELETE CASCADE 
         ON UPDATE CASCADE,
    FOREIGN KEY (BidderId) 
      REFERENCES User (UserId) 
         ON DELETE CASCADE 
         ON UPDATE CASCADE,
    FOREIGN KEY (ItemId) 
      REFERENCES Item (ItemId) 
         ON DELETE CASCADE 
         ON UPDATE CASCADE
);
