WITH CategoryCount(ItemID, Count) AS
    (SELECT category.ItemID AS ItemID, COUNT(category.Category) AS Count
    FROM Category category
    GROUP BY category.ItemID)
SELECT COUNT(item.ItemID)
FROM Item item, CategoryCount
WHERE item.ItemID = CategoryCount.ItemID
AND CategoryCount.Count = 4;