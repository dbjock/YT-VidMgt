import sys
import logging
import sqlite3
import datetime
from pathlib import Path
log = logging.getLogger(__name__)


def initDB(scriptPath, dbLoc=":memory:"):
    """Initialize temporary database in memory.

    Args:
        scriptPath (PathType): The script path for executing script to create tables in database

    Returns:
        [dbConnection]: The dbconnection to the database
    """
    log.debug(f'create working db {dbLoc}')
    try:
        conn = sqlite3.connect(
            dbLoc, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    except sqlite3.Error as errID:
        log.critical(
            f":InMEMdb: Database connection failure. ", exc_info=True)
        sys.exit(1)

    log.debug(f"init db scriptPath={scriptPath}")
    scripts = ['createInMem.sql']
    scriptDir = Path(scriptPath)
    for sFile in scripts:
        scriptFile = scriptDir / sFile
        log.debug(f"Executing {scriptFile}")
        _exeScriptFile(dbConn=conn, scriptFileName=f'{scriptFile}')

    return conn


def addVidRec(dbConn, vidRec):
    log.debug(f"adding vidRec: {vidRec}")
    sql = "INSERT INTO vidinfo (vid_ID, vid_url,channel_url,upload_date,vid_title,season,episode,dl_FileName) VALUES (:vid_ID,:vid_url,:channel_url,:upload_date,:vid_title,:season,:episode,:dl_filename)"
    theVals = {
        'vid_ID': vidRec.vid_ID,
        'vid_url': vidRec.vid_url,
        'channel_url': vidRec.channel_url,
        'upload_date': vidRec.upload_date,
        'description': vidRec.description,
        'vid_title': vidRec.vid_title,
        'season': vidRec.season,
        'episode': vidRec.episode,
        'dl_filename': vidRec.dl_file
    }
    result = _exeDML(dbConn, sql, theVals)
    if result[0] == 0:
        result[1] = f"vidRec id : {vidRec.vid_ID} added"
    else:
        log.debug(f"problem adding vidRec {result}.")

    log.debug(f"returning {result}")
    return result


def getSeasons2Update(dbConn):
    """Get the season which need to be updated.

    Args:
        dbConn (db Connection): db connection object to the database

    Returns:
        tuple dictionary list: seasons which need to be updated
        [{'season':2000},{'season':2001}...]
    """
    sql = "SELECT season FROM vidinfo WHERE episode is NULL GROUP by season ORDER BY season"
    # Build SQL and execute
    try:
        dbConn.row_factory = sqlite3.Row  # .keys() enabled for column names
        c = dbConn.cursor()
        c.execute(sql)
        results = c.fetchall()
    except:
        log.critical(
            f'Unexpected error executing sql: {sql}', exc_info=True)
        sys.exit(1)
    if results is None:
        log.debug(f"rows returned 0")
        return 0
    else:
        log.debug(f"rows returned {len(results)}")
        return results


def getVidRecsSeason(dbConn, season):
    """Get vid ID's for season which need to be updated..

    Args:
        dbConn (db Connection): db connection object to the database

    Returns:
        tuple dictionary list: vid_IDs
        [{'vid_id':'234asdf'},{'vid_id':'2aWedasdff'}...]
    """
    sql = f"SELECT vid_ID FROM vidinfo WHERE episode is NULL AND season={season} ORDER by upload_date"
    # Execute SQL
    try:
        dbConn.row_factory = sqlite3.Row
        c = dbConn.cursor()
        c.execute(sql)
        results = c.fetchall()
    except:
        log.critical(
            f'Unexpected error executing sql: {sql}', exc_info=True)
        sys.exit(1)
    if results is None:
        log.debug(f"rows returned 0")
        return 0
    else:
        log.debug(f"rows returned {len(results)}")
        return results


def getVidRow(dbConn, vid_ID):
    selectSQL = "SELECT vid_ID,vid_title,vid_url,channel_url,upload_date,season,episode,dl_Filename FROM vidinfo"
    whereSQL = "WHERE vid_ID=?"
    value = vid_ID
    # Build SQL and execute
    sql = f"{selectSQL} {whereSQL}"
    theVals = (value,)
    log.debug(f"sql = {sql}")
    log.debug(f"theVals = {theVals}")
    try:
        dbConn.row_factory = sqlite3.Row
        # enable full sql traceback
        dbConn.set_trace_callback(log.debug)
        c = dbConn.cursor()
        c.execute(sql, theVals)
        row = c.fetchone()
        # Disable full sql traceback
        dbConn.set_trace_callback(None)
    except:
        log.critical(
            f'Unexpected error executing sql: {sql}', exc_info=True)
        sys.exit(1)

    if results is None:
        log.debug(f"rows returned 0")
        return 0
    else:
        log.debug(f"rows returned {len(results)}")
        return results


def _exeScriptFile(dbConn, scriptFileName):
    """
    Executes a Script file. (internal use only)
    scriptFileName : SQL script file to run
    """
    log.debug(f"loading script {scriptFileName} to memory")
    scriptFile = open(scriptFileName, 'r')
    script = scriptFile.read()
    scriptFile.close()
    try:
        c = dbConn.cursor()
        c.executescript(script)
    except:
        log.critical(
            f":InMEMdb: Unexpected Error running script {scriptFileName}", exc_info=True)
        sys.exit(1)

    dbConn.commit()
    log.debug(f"script commited")


def _exeDML(dbConn, sql, theVals):
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
        c = dbConn.cursor()
        # Enabling full sql traceback to log.debug
        dbConn.set_trace_callback(log.debug)
        c.execute(sql, theVals)
        dbConn.commit()
    except sqlite3.IntegrityError as e:
        log.warning(f"sqlite integrity error: {e.args[0]}")
        return [2, f"sqlite integrity error: {e.args[0]}"]
    except:
        log.critical(
            f'Unexpected error executing sql: {sql}', exc_info=True)
        sys.exit(1)

    log.debug("successful commit of sql")
    # Disable full sql traceback to log.debug
    dbConn.set_trace_callback(None)
    return [0, "Commit successful"]
