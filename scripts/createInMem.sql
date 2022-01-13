-- Text encoding used: System
--
PRAGMA foreign_keys = off;
BEGIN TRANSACTION;

-- Table: main
CREATE TABLE vidinfo (
    vid_ID               PRIMARY KEY
                         NOT NULL,
    vid_title,
    vid_url,
    channel_url,
    upload_date DATETIME,
    season      INTEGER,
    episode     INTEGER,
    dl_FileName,
    json_FileName
);

COMMIT TRANSACTION;
PRAGMA foreign_keys = on;
