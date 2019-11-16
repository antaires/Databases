--Candidate number 97523
--Modelling Exercise Coursework 1

DROP TABLE IF EXISTS Review;
DROP TABLE IF EXISTS CategoryChart;
DROP TABLE IF EXISTS Category;
DROP TABLE IF EXISTS Part;
DROP TABLE IF EXISTS Member;

CREATE TABLE Member (
    memberID INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    image BLOB, 
    date INTEGER NOT NULL,
    password VARCHAR(100) NOT NULL
);

CREATE TABLE Part (
    partID INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    price FLOAT (10) NOT NULL,
    image BLOB,
    description VARCHAR(250)
);

CREATE TABLE Category (
    categoryID INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description VARCHAR(250)
);

CREATE TABLE CategoryChart (
    categoryID INTEGER NOT NULL,
    partID INTEGER NOT NULL,
    PRIMARY KEY (categoryID, partID),
    FOREIGN KEY (categoryID) REFERENCES Category(categoryID),
    FOREIGN KEY (partID) REFERENCES Part(partID)
);

CREATE TABLE Review (
    reviewID INTEGER PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    yearAdded INTEGER NOT NULL,
    text VARCHAR(2000),
    rating INTEGER NOT NULL, -- between 0 - 5 inclusive
    partID INTEGER NOT NULL,
    memberID INTEGER NOT NULL,
    FOREIGN KEY (partID) REFERENCES Part(partID),
    FOREIGN KEY (memberID) REFERENCES Member(memberID),
    CONSTRAINT CHK_RatingRange CHECK (rating >= 0 AND rating <= 5)
);
