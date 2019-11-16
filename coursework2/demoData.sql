INSERT INTO Person (name, username, studentId) VALUES
('Aurora', 'A', 'aaaaaa'),
('Haoyu', 'H', 'bbbbbb'),
('Valia', 'V', 'cccccc'),
('Sonam', 'S', 'dddddd');

INSERT INTO Forum (forumTitle) VALUES
('Forum 1'),
('Forum 2');

INSERT INTO Topic (topicTitle, forumId, timeCreated, lastPostTime) VALUES
('Topic 1', 1, '2019-03-25 14:31:22', '2019-03-25 14:32:22'),
('Topic 2', 1, '2019-03-25 14:31:22', '2019-03-25 14:32:22'),
('Topic 3', 2, '2019-03-25 14:33:22', '2019-03-25 14:33:22');

INSERT INTO Post (authorUsername, postText, timePosted, topicId, postNumberinTopic) VALUES 
('A', 'This is Post 1 content', '2019-03-25 14:31:22', 1, 1),
('H', 'This is Post 2 content', '2019-03-25 14:32:22', 2, 1),
('V', 'This is Post 3 content', '2019-03-25 14:33:22', 2, 2),
('S', 'This is Post 4 content', '2019-03-25 14:34:22', 3, 1);

INSERT INTO TopicLike (username, topicId) VALUES
('A', 1),
('S', 2);

INSERT INTO PostLike (username, postId) VALUES
('H', 1),
('A', 1),
('V', 2),
('S', 3);