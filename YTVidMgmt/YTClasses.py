# Module for interacting with app database [sqlite]
import sys
import logging
import sqlite3
import datetime
from pathlib import Path
# Custom App modules
logger = logging.getLogger(__name__)


class APPdb:
    def __init__(self, name=None):
        self.conn = None
        logger.debug(f'name is {name}')
        if name:
            logger.debug(f"attempt open db {name}")
            try:
                self.conn = sqlite3.connect(
                    name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            except sqlite3.Error as errID:
                logger.critical(
                    f"Database connection failure. ", exc_info=True)
                sys.exit(1)
            c = self.conn.cursor()
            c.execute("PRAGMA database_list;")
            xtmp = c.fetchall()
            # self.dbfile = xtmp[0][2]
            logger.debug(f"database_list={xtmp}")

    def chkDB(self):
        """Check database for required tables

        Returns:
            list: retCode, retDesc
            0, 'Database good'
            If retCode >0 then something wrong
        """
        logger.debug(f"Checking database")
        c = self.conn.cursor()
        # Check required database objects and if missing create.
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='vidinfo'"
        c.execute(sql)
        if c.fetchone() is None:
            logger.debug('Missing table: vidinfo')
            return [1, 'Missing table: vidinfo']
        else:
            logger.debug('Database good')
            return [0, 'Database good']

    def initDB(self, scriptPath=None):
        """Create tables, views, indexes

        PARM
        scriptPath : path to script files *Required
        """
        logger.debug(f"scriptPath={scriptPath}")
        scripts = ['createTables.sql']

        scriptDir = Path(scriptPath)

        for sFile in scripts:
            scriptFile = scriptDir / sFile
            logger.debug(f"Executing {scriptFile}")
            self._exeScriptFile(scriptFileName=f'{scriptFile}')

    def _exeScriptFile(self, scriptFileName=None):
        """
        Executes a Script file. (internal use only)
        scriptFileName : SQL script file to run
        """
        logger.debug(f"loading script {scriptFileName} to memory")
        scriptFile = open(scriptFileName, 'r')
        script = scriptFile.read()
        scriptFile.close()
        try:
            c = self.conn.cursor()
            c.executescript(script)
        except:
            logger.critical(
                f"Unexpected Error running script {scriptFileName}", exc_info=True)
            sys.exit(1)

        self.conn.commit()
        logger.debug(f"script commited")

    def _exeDML(self, sql, theVals):
        """Executes INSERT, DELETE, UPDATE sql. (internal use only)

        ARGS
        sql : The insert Sql to use.
        theVals   : The value parms passed into the sql

        Returns - list (ResultCode, ResultText)
                ResultCode 0 = Success execution
                Resultcode != 0 - See ResultText for details
        """
        logger.debug(f"Sql: {sql}")
        logger.debug(f"Values: {theVals}")
        try:
            c = self.conn.cursor()
            # Enabling full sql traceback to logger.debug
            self.conn.set_trace_callback(logger.debug)
            c.execute(sql, theVals)
            self.conn.commit()
        except sqlite3.IntegrityError as e:
            logger.warning(f"sqlite integrity error: {e.args[0]}")
            return [2, f"sqlite integrity error: {e.args[0]}"]
        except:
            logger.critical(
                f'Unexpected error executing sql: {sql}', exc_info=True)
            sys.exit(1)

        logger.debug("successful commit of sql")
        # Disable full sql traceback to logger.debug
        self.conn.set_trace_callback(None)
        return [0, "Commit successful"]

    def getLastEpisode(self):
        """Gets highest episode number in database

        Returns:
            int: highest episode number
        """
        c = self.conn.cursor()
        sql = "SELECT episode FROM vidinfo ORDER BY episode DESC LIMIT 1"
        logger.debug(f"SQL = {sql}")
        c.execute(sql)
        result = c.fetchone()
        if result is None:
            logger.debug("No records. Last episode = 0")
            return 0
        else:
            logger.debug(f"result = {result}")
            return result[0]

    def getVid(self, vid_ID):
        selectSQL = "SELECT vid_ID, vid_url,channel_url,upload_date,description,vid_title,episode FROM vidinfo"
        whereSQL = "WHERE vid_ID=?"
        value = vid_ID

        # Build SQL and execute
        sql = f"{selectSQL} {whereSQL}"
        theVals = (value,)
        logger.debug(f"sql = {sql}")
        logger.debug(f"theVals = {theVals}")
        try:
            self.conn.row_factory = sqlite3.Row  # .keys() enabled for column names
            # enable full sql trackback to logger.debug
            self.conn.set_trace_callback(logger.debug)
            c = self.conn.cursor()
            c.execute(sql, theVals)
            row = c.fetchone()
            # Disable full sql traceback
            self.conn.set_trace_callback(None)
        except:
            logger.critical(
                f'Unexpected error executing sql: {sql}', exc_info=True)
            sys.exit(1)
        # row is the results, now evaluate
        if row:  # have data
            logging.debug(
                "Record found. Creating VidRecObject with data to return")
            vRecord = VidRec(vid_ID)
            vRecord.vid_ID = row['vid_ID']
            vRecord.vid_url = row['vid_url']
            vRecord.vid_title = row['vid_title']
            vRecord.channel_url = row['channel_url']
            vRecord.upload_date = row['upload_date']
            vRecord.episode = row['episode']
            vRecord.description = row['description']

        else:  # no data, create empty vRecord
            logging.debug(
                "No Record found. Creating blank VidRecObject to return")
            vRecord = VidRec(0)

        return vRecord

    def addVidRec(self, vidRec):
        """Create/Save Video Record to db
        Args:
            vidRec ([type]): [description]

        Returns:
            list: (resultCode, resultText)
            resultCode = 0 success
            resultCode > 0 unsuccessfull. See resultText
        """
        logger.debug(f"add VidRec")
        sql = "INSERT INTO vidinfo (vid_ID, vid_url,channel_url,upload_date,description,vid_title,episode) VALUES (:vid_ID,:vid_url,:channel_url,:upload_date,:description,:vid_title,:episode)"
        theVals = {'vid_ID': vidRec.vid_ID, 'vid_url': vidRec.vid_url, 'channel_url': vidRec.channel_url,
                   'upload_date': vidRec.upload_date, 'description': vidRec.description, 'vid_title': vidRec.vid_title, 'episode': vidRec.episode}
        r = self._exeDML(sql, theVals)
        if r[0] == 0:
            r[1] = f"vidRec id : {vidRec.vid_ID} added"
        else:
            logger.debug(f"problem with adding vidRec {r}.")

        logger.debug(f"returning {r}")
        return r


class VidRec:
    def __init__(self, vid_ID):
        self.vid_ID = vid_ID
        self.vid_url = None
        self.vid_title = None
        self.channel_url = None
        self.upload_date = None
        self.description = None
        self.episode = None
        self.dl_file = None  # download file name

    def __str__(self):
        return f' vid_id = {self.vid_ID}\n vid_title = {self.vid_title}\n vid_url = {self.vid_url}\n channel_url = {self.channel_url}\n upload_date = {self.upload_date}\n episode = {self.episode}\n'
