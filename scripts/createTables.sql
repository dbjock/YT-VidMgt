-- Text encoding used: System
--
PRAGMA foreign_keys = off;
BEGIN TRANSACTION;

-- Table: main
CREATE TABLE vidinfo (
    vid_ID               PRIMARY KEY
                         NOT NULL,
    vid_url,
    channel_url,
    upload_date DATETIME,
    description,
    vid_title,
    episode     INTEGER  UNIQUE
);

COMMIT TRANSACTION;
PRAGMA foreign_keys = on;
