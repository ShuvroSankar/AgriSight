DROP TABLE IF EXISTS elements;

CREATE TABLE elements (
    element_code INT NOT NULL PRIMARY KEY,
    element NVARCHAR(100) NOT NULL
);

INSERT INTO elements (element_code, element) VALUES
(5312,'Area harvested'),
(5423,'Extraction Rate'),
(5313,'Laying'),
(5318,'Milk Animals'),
(5319,'Prod Population'),
(5314,'Prod Population'),
(5320,'Producing Animals/Slaughtered'),
(5321,'Producing Animals/Slaughtered'),
(5322,'Production'),
(5323,'Production'),
(5510,'Production'),
(5513,'Production'),
(5111,'Stocks'),
(5112,'Stocks'),
(5114,'Stocks'),
(5420,'Yield'),
(5422,'Yield'),
(5410,'Yield'),
(5412,'Yield'),
(5413,'Yield'),
(5417,'Yield/Carcass Weight'),
(5424,'Yield/Carcass Weight');
