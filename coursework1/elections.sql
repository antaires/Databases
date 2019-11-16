-- The comment lines starting -- ! are used by the automatic testing tool to
-- help mark your coursework. You must not modify them or add further lines
-- starting with -- !. Of course you can create comments of your own, just use
-- the normal -- to start them.

-- !elections

-- !question0
-- This is an example question and answer.

SELECT Party.name FROM Party 
JOIN Candidate ON Candidate.party = Party.id 
WHERE Candidate.name = 'Mark Bradshaw';

-- !question1

SELECT name FROM Party 
ORDER BY name ASC;

-- !question2

SELECT SUM(votes) AS totalVotes FROM Candidate; 

-- !question3

SELECT Candidate.name, Candidate.votes FROM Candidate
JOIN Ward ON Ward.id = Candidate.ward
WHERE Ward.name = 'Bedminster';

-- !question4

SELECT SUM(Candidate.votes) AS totalVotes FROM Candidate
JOIN Ward ON Ward.id = Candidate.ward
JOIN Party ON Party.id = Candidate.party
WHERE Party.name = 'Liberal Democrat' AND Ward.name = 'Filwood';

-- !question5

SELECT Candidate.name, Party.name AS party, votes
FROM Candidate
JOIN Ward ON Ward.id = Candidate.ward
JOIN Party ON Party.id = Candidate.party
WHERE Ward.name = 'Hengrove'
ORDER BY votes DESC;

-- !question6

SELECT COUNT(*) FROM Candidate
JOIN Party ON Party.id = Candidate.party
JOIN Ward ON Ward.id = Candidate.ward
WHERE Ward.name = 'Bishopsworth' AND Candidate.votes >= (SELECT Candidate.votes FROM Candidate JOIN Ward ON Ward.id = Candidate.ward JOIN Party ON Party.id = Candidate.party WHERE Ward.name = 'Bishopsworth' AND Party.name = 'Labour');

-- !question7

SELECT Votes.wardName, (Candidate.votes / Votes.votesPerWard) * 100 AS SuccessPercent
FROM Candidate
JOIN (
	SELECT Ward.name AS wardName, Ward.id AS wardID, SUM(Candidate.votes) AS votesPerWard
	FROM Candidate
	JOIN Ward ON Ward.id = Candidate.ward
	GROUP BY Ward.id
	ORDER BY Ward.name ASC ) AS Votes ON Votes.wardID = Candidate.ward
JOIN Party ON Party.id = Candidate.party
WHERE Party.name = 'Green'
GROUP BY Votes.wardID, Party.name
ORDER BY Votes.wardName ASC;

-- !question8

SELECT Summary.wardName AS ward, (Summary.greenVotes/Summary.electorate * 100)-(Summary.labourVotes/Summary.electorate *100) AS rel, Summary.greenVotes - Summary.labourVotes AS Abs 
FROM ( SELECT Ward.name AS wardName, Candidate.votes AS greenVotes, LabourVotes.votes AS LabourVotes, Ward.electorate AS electorate 
	FROM Candidate 
	JOIN Party ON Party.id = Candidate.party 
JOIN Ward ON Ward.id = Candidate.ward 
JOIN (SELECT Ward.name AS wardName, Candidate.votes AS votes 
	FROM Candidate JOIN Party ON Party.id = Candidate.party 
	JOIN Ward ON Ward.id = Candidate.ward WHERE Party.name = 'Labour') AS LabourVotes ON LabourVotes.wardName = Ward.name
	WHERE Party.name = 'Green'
	ORDER BY Ward.name ) AS Summary
WHERE Summary.greenVotes > Summary.labourVotes
ORDER BY Summary.wardName;

-- !end
