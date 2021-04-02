# Module for interacting with app database [sqlite]
import sys
import logging
import sqlite3
import datetime
from pathlib import Path
# Custom App modules
log = logging.getLogger(__name__)


class APPdb:
    def __init__(self, name=None):
        self.conn = None
        self.dbName = name
        log.debug(f'name is {name}')
        if name:
            log.debug(f"attempt open db {name}")
            try:
                self.conn = sqlite3.connect(
                    name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            except sqlite3.Error as errID:
                log.critical(
                    f"Database connection failure. ", exc_info=True)
                sys.exit(1)
            c = self.conn.cursor()
            c.execute("PRAGMA database_list;")
            xtmp = c.fetchall()
            log.debug(f"database_list={xtmp}")

    def chkDB(self):
        """Check database for required tables

        Returns:
            list: retCode, retDesc
            0, 'Database good'
            If retCode >0 then something wrong
        """
        log.debug(f"Checking database")
        c = self.conn.cursor()
        # Check required database objects and if missing create.
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='vidinfo'"
        c.execute(sql)
        if c.fetchone() is None:
            log.debug('Missing table: vidinfo')
            return [1, 'Missing table: vidinfo']
        else:
            log.debug('Database good')
            return [0, 'Database good']

    def initDB(self, scriptPath=None):
        """Create tables, views, indexes

        PARM
        scriptPath : path to script files *Required
        """
        log.debug(f"scriptPath={scriptPath}")
        scripts = ['createTables.sql']

        scriptDir = Path(scriptPath)

        for sFile in scripts:
            scriptFile = scriptDir / sFile
            log.debug(f"Executing {scriptFile}")
            self._exeScriptFile(scriptFileName=f'{scriptFile}')

    def _exeScriptFile(self, scriptFileName=None):
        """
        Executes a Script file. (internal use only)
        scriptFileName : SQL script file to run
        """
        log.debug(f"loading script {scriptFileName} to memory")
        scriptFile = open(scriptFileName, 'r')
        script = scriptFile.read()
        scriptFile.close()
        try:
            c = self.conn.cursor()
            c.executescript(script)
        except:
            log.critical(
                f"Unexpected Error running script {scriptFileName}", exc_info=True)
            sys.exit(1)

        self.conn.commit()
        log.debug(f"script commited")

    def _exeDML(self, sql, theVals):
        """Executes INSERT, DELETE, UPDATE sql. (internal use only)

        ARGS
        sql : The insert Sql to use.
        theVals   : The value parms passed into the sql

        Returns - list (ResultCode, ResultText)
                ResultCode 0 = Success execution
                Resultcode != 0 - See ResultText for details
        """
        log.debug(f"Sql: {sql}")
        log.debug(f"Values: {theVals}")
        try:
            c = self.conn.cursor()
            # Enabling full sql traceback to log.debug
            self.conn.set_trace_callback(log.debug)
            c.execute(sql, theVals)
            self.conn.commit()
        except sqlite3.IntegrityError as e:
            log.warning(f"sqlite integrity error: {e.args[0]}")
            return [2, f"sqlite integrity error: {e.args[0]}"]
        except:
            log.critical(
                f'Unexpected error executing sql: {sql}', exc_info=True)
            sys.exit(1)

        log.debug("successful commit of sql")
        # Disable full sql traceback to log.debug
        self.conn.set_trace_callback(None)
        return [0, "Commit successful"]

    def getLastEpisode(self, season):
        """Gets highest episode number in database

        Returns:
            int: highest episode number
        """
        c = self.conn.cursor()
        selectSQL = "SELECT episode FROM vidinfo "
        whereSQL = f"WHERE season=? ORDER BY episode DESC LIMIT 1"
        theVals = (season,)
        log.debug(f"{self.dbName}: values = {theVals}")
        # Build SQL and execute
        sql = f"{selectSQL} {whereSQL}"
        try:
            self.conn.row_factory = sqlite3.Row  # .keys() enabled for column names
            # enable full sql trackback to log.debug
            self.conn.set_trace_callback(log.debug)
            c = self.conn.cursor()
            c.execute(sql, theVals)
            results = c.fetchone()
            # Disable full sql traceback
            self.conn.set_trace_callback(None)
        except:
            log.critical(
                f'Unexpected error executing sql: {sql}', exc_info=True)
            sys.exit(1)

        if results is None:
            log.debug("No records. returning 0")
            return 0
        else:
            log.debug(f"returning {results[0]}")
            return results[0]

    def getSeasons2Update(self):
        c = self.conn.cursor()
        sql = "SELECT season FROM vidinfo WHERE episode is NULL GROUP by season ORDER BY season"
        # Build SQL and execute
        try:
            self.conn.row_factory = sqlite3.Row  # .keys() enabled for column names
            # enable full sql trackback to log.debug
            self.conn.set_trace_callback(log.debug)
            c = self.conn.cursor()
            c.execute(sql)
            results = c.fetchall()
            # Disable full sql traceback
            self.conn.set_trace_callback(None)
        except:
            log.critical(
                f'Unexpected error executing sql: {sql}', exc_info=True)
            sys.exit(1)
        if results is None:
            log.debug(f"{self.dbName}: rows returned 0")
            return 0
        else:
            log.debug(f"{self.dbName}: rows returned {len(results)}")
            return results

    def getVid(self, vid_ID):
        selectSQL = "SELECT vid_ID,vid_title,vid_url,channel_url,upload_date,season,episode FROM vidinfo"
        whereSQL = "WHERE vid_ID=?"
        value = vid_ID

        # Build SQL and execute
        sql = f"{selectSQL} {whereSQL}"
        theVals = (value,)
        log.debug(f"sql = {sql}")
        log.debug(f"theVals = {theVals}")
        try:
            self.conn.row_factory = sqlite3.Row  # .keys() enabled for column names
            # enable full sql trackback to log.debug
            self.conn.set_trace_callback(log.debug)
            c = self.conn.cursor()
            c.execute(sql, theVals)
            row = c.fetchone()
            # Disable full sql traceback
            self.conn.set_trace_callback(None)
        except:
            log.critical(
                f'Unexpected error executing sql: {sql}', exc_info=True)
            sys.exit(1)
        # row is the results, now evaluate
        if row:  # have data
            log.debug(
                "Record found. Creating VidRecObject with data to return")
            vRecord = VidRec(vid_ID)
            vRecord.vid_ID = row['vid_ID']
            vRecord.vid_url = row['vid_url']
            vRecord.vid_title = row['vid_title']
            vRecord.channel_url = row['channel_url']
            vRecord.upload_date = row['upload_date']
            vRecord.season = row['season']
            vRecord.episode = row['episode']

        else:  # no data, create empty vRecord
            log.debug(
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
        log.debug(f"add VidRec")
        sql = "INSERT INTO vidinfo (vid_ID, vid_url,channel_url,upload_date,description,vid_title,episode) VALUES (:vid_ID,:vid_url,:channel_url,:upload_date,:description,:vid_title,:episode)"
        theVals = {'vid_ID': vidRec.vid_ID, 'vid_url': vidRec.vid_url, 'channel_url': vidRec.channel_url,
                   'upload_date': vidRec.upload_date, 'description': vidRec.description, 'vid_title': vidRec.vid_title, 'episode': vidRec.episode}
        r = self._exeDML(sql, theVals)
        if r[0] == 0:
            r[1] = f"vidRec id : {vidRec.vid_ID} added"
        else:
            log.debug(f"problem with adding vidRec {r}.")

        log.debug(f"returning {r}")
        return r


class VidRec:
    def __init__(self, vid_ID):
        self.vid_ID = vid_ID
        self.vid_url = None
        self.vid_title = None
        self.channel_url = None
        self.upload_date = None
        self.description = None
        self.season = None
        self.episode = None
        self.dl_file = None  # download file name

    def __str__(self):
        return f'vid_id={self.vid_ID}, vid_title={self.vid_title}, vid_url={self.vid_url}, channel_url={self.channel_url}, upload_date={self.upload_date}, season={self.season}, episode={self.episode},'
