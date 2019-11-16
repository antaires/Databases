-- This file is for your solutions to the census question.
-- Lines starting with -- ! are recognised by a script that we will
-- use to automatically test your queries, so you must not change
-- these lines or create any new lines starting with this marker.
--
-- You may break your queries over several lines, include empty
-- lines and SQL comments wherever you wish.
--
-- Remember that the answer to each question goes after the -- !
-- marker for it and that the answer to each question must be
-- a single SQL query.
--
-- Upload this file to SAFE as part of your coursework.

-- !census

-- !question0

-- Sample solution to question 0.
SELECT data FROM Statistic WHERE wardId = 'E05001982' AND
occId = 2 AND gender = 0;

-- !question1

SELECT data FROM Statistic WHERE occID = 7 AND gender = 1 AND wardID = 'E05001975';

-- !question2

SELECT SUM(data) FROM Statistic WHERE occID = 5 AND wardID = 'E05000697';

-- !question3

SELECT SUM(data) AS num_people, Occupation.name AS occ_class
FROM Statistic
JOIN Occupation ON Occupation.id = Statistic.occid
JOIN Ward ON Ward.code = Statistic.wardId
WHERE Ward.code = 'E05008884'
GROUP BY Occupation.name;

-- !question4

SELECT SUM(data) AS 'Working Population', Ward.code, Ward.name AS 'Ward Name', County.name AS 'County'
FROM Statistic 
JOIN Ward ON Ward.code = Statistic.wardId
JOIN County ON County.code = Ward.parent
GROUP BY Ward.name, Ward.code, County.name
ORDER BY workingPop ASC
LIMIT 1;

-- !question5

SELECT COUNT(Pop.workingPop) AS 'wards with at least 1000' 
FROM ( SELECT SUM(data) AS workingPop
	FROM Statistic
	JOIN Ward ON Ward.code = Statistic.wardId
	GROUP BY Ward.code HAVING workingPop >= 1000 ) AS Pop;

-- !question6

SELECT Region.name AS name, AVG(Pop.popSUM) AS avg_size
FROM (SELECT SUM(data) AS popSum, Ward.code AS wardCode
	FROM Statistic
	JOIN Ward ON Ward.code = Statistic.wardId
	GROUP BY Ward.code) AS Pop
JOIN Ward ON Ward.code = Pop.wardCode
JOIN County ON County.code = Ward.parent
JOIN Region ON Region.code = County.parent
JOIN Country ON Country.code = County.country
WHERE Country.name = 'England'
GROUP BY Region.code;

-- !question7

SELECT Fulldata.co AS CLU, Fulldata.occ AS occupation, (SELECT CASE Fulldata.g WHEN 0 then 'male' else 'female' END) AS gender, Fulldata.d AS N
FROM ( SELECT County.name AS co, Occupation.name AS occ, Statistic.gender AS g, SUM(Statistic.data) AS d
	FROM Statistic
	JOIN Occupation ON Occupation.id = Statistic.occid
	JOIN Ward ON Ward.code = Statistic.wardId
	JOIN County ON County.code = Ward.parent
	WHERE County.parent = 'E12000002'
	GROUP BY County.name, Occupation.name, Statistic.gender) AS Fulldata
WHERE Fulldata.d > 10000
ORDER BY Fulldata.d ASC;

-- !question8

SELECT Total.region, SUM(CASE WHEN Total.gender=0 THEN Total.total ELSE 0 END) AS men, SUM(CASE WHEN Total.gender=1 THEN Total.total ELSE 0 END) AS women, SUM(CASE WHEN Total.gender=1 THEN Total.total ELSE 0 END) / SUM(Total.total) AS proportion
FROM (SELECT Region.name AS region, SUM(Statistic.data) AS total, Statistic.gender AS gender
	FROM Region
	JOIN County ON Region.code = County.parent
	JOIN Ward ON Ward.parent = County.code
	JOIN Statistic ON Statistic.wardId = Ward.code
	WHERE Statistic.occId = 1
	GROUP BY Region.name, Statistic.gender ) AS Total
GROUP BY Total.region;

-- !question9

SELECT Region.name AS name, AVG(Pop.popSUM) AS avg_size
FROM (SELECT SUM(data) AS popSum, Ward.code AS wardCode FROM Statistic JOIN Ward ON Ward.code = Statistic.wardId GROUP BY Ward.code) AS Pop
JOIN Ward ON Ward.code = Pop.wardCode
JOIN County ON County.code = Ward.parent
JOIN Region ON Region.code = County.parent
JOIN Country ON Country.code = County.country
WHERE Country.name = 'England'
GROUP BY Region.code
UNION ALL
SELECT 'England', AVG(Pop.popSum) AS 'England' FROM (SELECT SUM(data) AS popSum FROM Statistic JOIN Ward ON Ward.code = Statistic.wardId GROUP BY Ward.code) AS Pop 
UNION ALL
SELECT 'All', AVG(Pop.popSum) AS 'All' FROM (SELECT SUM(data) AS popSum FROM Statistic JOIN Ward ON Ward.code = Statistic.wardId
	JOIN County ON County.code = Ward.parent
	JOIN Region ON Region.code = County.parent
	JOIN Country ON Country.code = County.country
	WHERE Country.name = 'England'
	GROUP BY Ward.code) AS Pop;

-- !end
