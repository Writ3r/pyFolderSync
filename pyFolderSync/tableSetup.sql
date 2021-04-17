CREATE TABLE IF NOT EXISTS sync (
    folderIn VARCHAR(100),
    folderOut VARCHAR(100),
    PRIMARY KEY (folderIn, folderOut)
);

CREATE TABLE IF NOT EXISTS location (
    folderIn VARCHAR(100),
    folderOut VARCHAR(100),
    folderInLocation VARCHAR(100),
    folderInId VARCHAR(100),
    FOREIGN KEY(folderIn, folderOut) REFERENCES sync(folderIn, folderOut),
    PRIMARY KEY (folderIn, folderOut, folderInLocation)
);
