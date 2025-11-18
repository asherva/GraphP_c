DECLARE @s VARCHAR(200) = 'abc#one#two#three#end';

SELECT SUBSTRING(
        @s,
        CHARINDEX('#', @s, CHARINDEX('#', @s) + 1) + 1,
        CHARINDEX('#', @s, CHARINDEX('#', @s, CHARINDEX('#', @s) + 1) + 1)
          - CHARINDEX('#', @s, CHARINDEX('#', @s) + 1) - 1
       ) AS Result;
