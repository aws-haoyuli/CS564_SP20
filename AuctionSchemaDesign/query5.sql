SELECT COUNT(user.UserId)
FROM User user
WHERE user.UserId IN
    (SELECT SellerId
    FROM Bid)
AND user.Rating > 1000;