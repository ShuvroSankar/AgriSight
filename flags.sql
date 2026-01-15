DROP TABLE IF EXISTS flags;

CREATE TABLE flags (
    flag CHAR(1) NOT NULL PRIMARY KEY,
    description NVARCHAR(200) NOT NULL
);

INSERT INTO flags (flag, description) VALUES
('A','Official figure'),
('E','Estimated value'),
('I','Imputed value'),
('M','Missing value (data cannot exist; not applicable)'),
('X','Figure from international organizations');
