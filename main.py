import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import shutil
from datetime import datetime
from pathlib import Path
import argparse
import copy
import json

# App Custom modules
from YTVidMgmt import YTClasses
from YTVidMgmt import memdb

APP_VER = "1.21"

# Log Formatters
smlFMT = logging.Formatter(
    '%(asctime)s %(levelname)-8s %(message)s')
extFMT = logging.Formatter(
    '%(asctime)s %(levelname)-8s:%(name)s.%(funcName)s: %(message)s')
# Log Handlers
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(smlFMT)
# Initilizing logging (This is the root logger now)
log = logging.getLogger('')
log.setLevel(logging.DEBUG)
log.addHandler(console)
appPath = Path(__file__).parent
scriptPath = appPath / 'Scripts'


def logTest():
    log.debug("logtesting-I am a debug entry")
    log.info("logtesting-I am an info entry")
    log.critical("logtesting-I am a critical entry")
    log.warning("logtesting-I am a warning entry")


def json2VidRec(jsonFile):
    """Creates vidRec object from jsonFile

    Args:
        jsonFile (Path obj): json file to load

    Returns:
        VidRec obj: video record class object
    """
    vidRec = YTClasses.VidRec(0)
    # confirm file exists
    if not jsonFile.exists():
        log.warning(f"{jsonFile} does not exist")
        return vidRec

    # Load json
    with open(jsonFile) as jFile:
        jData = json.load(jFile)
        log.debug(f"Loaded file: {jsonFile}")
        vidRec.vid_ID = jData['id']
        vidRec.vid_url = jData['webpage_url']
        vidRec.channel_url = jData['channel_url']
        # Convert the YYYYMMDD to YYYY-MM-DD for vidRec
        uploadDate = datetime.strptime(jData['upload_date'], '%Y%m%d')
        vidRec.upload_date = uploadDate.strftime('%Y-%m-%d')
        vidRec.season = uploadDate.strftime('%Y')
        vidRec.vid_title = jData['title']
        vidRec.dl_file = jData['_filename']
        vidRec.json_file = str(jsonFile)

    return vidRec


def vidRow2VidRec(vidRowData):
    """Creates vidRec object from vidRow

    Args:
        vidRowData (dictType): row data of video

    Returns:
        VidRec obj: video record class object
    """
    vidRec = YTClasses.VidRec(0)
    vidRec.vid_ID = vidRowData['vid_ID']
    vidRec.vid_title = vidRowData['vid_title']
    vidRec.vid_url = vidRowData['vid_url']
    vidRec.channel_url = vidRowData['channel_url']
    vidRec.upload_date = vidRowData['upload_date']
    vidRec.season = vidRowData['season']
    vidRec.episode = vidRowData['episode']
    vidRec.dl_file = vidRowData['dl_Filename']
    vidRec.json_file = vidRowData['json_FileName']

    return vidRec


def createMetaFile(vidRec, metafName):
    """Creates metafName based on vidRec object
     file format criteria https://bitbucket.org/mjarends/extendedpersonalmedia-agent.bundle/src/0982485ee6d54b5b927434210bd694f29a159ef7/Samples/show.metadata

    Args:
        vidRec (VidRec object class): Video record object
        metafName (str): Full Path and filename of meta file to create
    """
    # Determine output metafile name
    logMsg = "vid_ID: {vidRec.vid_ID}"
    log.debug(f"vid_ID: {vidRec.vid_ID}, metafName={metafName}")
    log.debug(f"Writing to {metafName}")
    with open(metafName, 'w') as oFile:
        oFile.write("[metadata]\n")
        oFile.write(f"title={vidRec.vid_title}\n")
        oFile.write(f"release={vidRec.upload_date}\n")
        # Summary data not used. Youtubers can put bad stuff in here. Need to code 'cleansing' of this data.
    log.info(f"vid_ID: {vidRec.vid_ID}, created {metafName}")


def cleanStr(dirtyStr):
    """Replaces $%&*:@'\/" in a string with an underscore

    Args:
        dirtyStr (str): The string to clean

    Returns:
        str: the string cleaned
    """
    badChar = ["$", "!", "%", "&", "*", ":", "@", "'", "\\", "/"]
    log.debug(f"Cleaning string: {dirtyStr}")
    # encode so on ASCII characters
    encoded_string = dirtyStr.encode("ascii", "ignore")
    decode_string = encoded_string.decode()
    log.debug(f"stripped to ASCII decode_string: {decode_string}")

    # Replace badChar in ascII only string
    for b in badChar:
        decode_string = decode_string.replace(b, "_")

    log.debug(f"Cleaned string : {decode_string}")
    return decode_string


def calcFilename(vidRec, YTChannel):
    """Creates a base filename based on factors in the vidRec"""
    # YouTubeChannel_SnnnnEnnn_yyyy-mm-dd_title.id.extension
    # Get a clean title
    cleanTitle = cleanStr(vidRec.vid_title)
    season = vidRec.upload_date[:4]
    episode = str(vidRec.episode).zfill(3)
    upload_date = vidRec.upload_date
    vidID = vidRec.vid_ID
    log.debug(
        f"cleanTitle={cleanTitle},season={season},episode={episode},upload_date={upload_date},vidID={vidID},dl_file={vidRec.dl_file}")

    return f"{YTChannel} - S{season}E{episode} - {cleanTitle}.{vidID}"


def json2memDb(inMemDbconn, diskDb):
    log.info("--- Metadata json files being loaded. ---")
    # Create list of all json files
    jsonFiles = []
    log.debug(f"getting count of json files")
    srcFolder = Path(args.inFolder)
    files = srcFolder.rglob('*.json')
    for x in files:
        jsonFiles.append(x)

    log.debug(f"movie metadata files found: {len(jsonFiles)}")
    if len(jsonFiles) == 0:
        log.info("No metadata files found")

    # Read jsonfile and update in memory database, which will be used to determine filenames.
    curFnum = 1
    for jsonFile in jsonFiles:
        log.info(f"Loading file {curFnum} of {len(jsonFiles)}: {jsonFile}")
        curVidRec = json2VidRec(jsonFile)
        # Check db to see if video record object id exists
        log.debug(
            f"Checking disk db for ({curVidRec.vid_ID}) {curVidRec.vid_title}")
        dbVidRec = diskDb.getVid(curVidRec.vid_ID)
        if dbVidRec.vid_ID == curVidRec.vid_ID:  # exists in db
            log.warning(
                f"({curVidRec.vid_ID}) {curVidRec.vid_title} exists in db. meta data will be ignored")
            log.debug("replace vidrec with db vidrec object")
            # store download video file name
            dbVidRec.dl_file = curVidRec.dl_file
            # Replace cur video obj with db one
            del curVidRec
            curVidRec = copy.copy(dbVidRec)
            del dbVidRec
        else:  # does not exist in db
            log.debug(
                f"({curVidRec.vid_ID}) {curVidRec.vid_title} does not exist in db")

        # Adding to database
        result = memdb.addVidRec(inMemDbconn, curVidRec)
        if result[0] != 0:  # Failure adding
            log.critical(
                f"Unable to save video record. vid_id: {curVidRec.vid_ID}, vidFile: {curVidRec.dl_file}")
            log.critical(f"Return Code: {result}")
            sys.exit(1)
        curFnum += 1


def createFiles(inMemDbconn, diskDb):
    """Creates vids and meta files queued from inMemDB

    Args:
        inMemDbconn ([type]): [description]
        diskDb ([type]): [description]
    """
    log.info(f"--- Creating files in {args.outFolder} ---")
    # Create the meta files, and vids using the inMemDB
    vidsRecs2Process = memdb.getAllVidRows(inMemDbconn)
    vCount = 1
    for vidID in vidsRecs2Process:
        vidRowData = memdb.getVidRow(inMemDbconn, vidID[0])
        log.debug(f"vidRowData:")
        for k in vidRowData.keys():
            log.debug(f"{k}={vidRowData[k]}")
        curVidRec = vidRow2VidRec(vidRowData)
        log.debug(
            f"---- start {vCount} of {len(vidsRecs2Process)} vid_ID: {curVidRec.vid_ID}")

        # Set origin and destination directories
        originDir = Path(curVidRec.dl_file).parent
        destDir = Path(args.outFolder)
        log.debug(f"originDir={originDir}, destDir = {destDir}")

        # Create destDir if it doesnt exist
        log.debug(f"checking if destDir={destDir} exists")
        if not destDir.exists():
            log.warning(f"Creating {destDir}")
            destDir.mkdir(parents=True, exist_ok=True)

        # Set baseFilename
        baseFilename = calcFilename(curVidRec, Path(args.inFolder).name)
        log.debug(f"baseFilename = {baseFilename}")

        # Set destination metafilename
        xFilename = baseFilename + ".metadata"
        destMetaFileName = destDir / Path(xFilename)

        # Set destination Video file name
        xFilename = baseFilename + Path(curVidRec.dl_file).suffix
        destVidFileName = destDir / xFilename

        srcVidFileName = Path(curVidRec.dl_file)
        if curVidRec.json_file:
            srcJsonFileName = Path(curVidRec.json_file)
        else:
            srcJsonFileName = None

        if srcVidFileName.exists():  # Create files since vid file exists
            # Create destination metafile
            createMetaFile(curVidRec, destMetaFileName)
            log.info(
                f"Metadata file {vCount} of {len(vidsRecs2Process)} vid_ID: {curVidRec.vid_ID}, created  {destMetaFileName}")
            # Create destination video file
            logmsg_prefix = f"Video file {vCount} of {len(vidsRecs2Process)} vid_ID: {curVidRec.vid_ID}"
            if args.copyOnly:
                log.debug(f"copying {srcVidFileName} to {destVidFileName}")
                try:
                    shutil.copy2(src=srcVidFileName, dst=destVidFileName)
                except:
                    log.critical(f"{logmsg_prefix} Unexpected error when trying to copy {srcVidFileName} -> {destVidFileName}",exc_info=True)
                    sys.exit(1)
                logMsg = f"{logmsg_prefix} COPIED {srcVidFileName} -> {destVidFileName}"
            else:
                try:
                    shutil.move(src=srcVidFileName, dst=destVidFileName)
                except:
                    log.critical(f"{logmsg_prefix} Unexpected error when trying to move {srcVidFileName} -> {destVidFileName}",exc_info=True)
                    sys.exit(1)
                logMsg = f"{logmsg_prefix} moved {srcVidFileName} -> {destVidFileName}"
                # delete json file as no longer needed
                if srcJsonFileName:
                    log.debug(f"deleted {srcJsonFileName}")
                    srcJsonFileName.unlink
            log.info(f"{logMsg}")

            # Update ondisk DB
            result = diskDb.addVidRec(curVidRec)
            log.debug(f"Result from updating appDB: {result}")
        else:  # video file does not exist (do not create files)
            log.warning(
                f"{vCount} of {len(vidsRecs2Process)} vid_ID: {curVidRec.vid_ID}, {srcVidFileName} file missing - Skipped")

        vCount += 1


def main(args):
    if args.logFile:
        log_fh = RotatingFileHandler(
            args.logFile, mode='a', maxBytes=1048576, backupCount=2)
        extFMT = logging.Formatter(
            '%(asctime)s %(levelname)-8s:%(name)s.%(funcName)s: %(message)s')
        log_fh.setFormatter(extFMT)
        log_fh.setLevel(logging.DEBUG)
        # Add logging filehander log_fh to the logger
        log.addHandler(log_fh)

    log.info("======= START ======= ")
    log.info(f"Ver={APP_VER}")
    log.info(f"appPath      : {appPath}")
    log.info(f"scriptPath   : {scriptPath}")
    log.info(f"LogFile      : {args.logFile}")
    log.debug(f"args={args}")
    log.info(f"In Directory : {args.inFolder}")
    log.info(f"Out Directory: {args.outFolder}")
    log.info(f"Database File: {args.dbLoc}")
    if args.copyOnly:
        log.info(f"   *COPY ONLY enabled")

    # Cleaning up for inMem work db. It may have been on disk
    dbLoc = Path(args.dbLoc).parent / "inMem.tmp"
    if dbLoc.exists():
        log.debug(f"Removing {dbLoc}")
        dbLoc.unlink()

    if not args.noInMemDb:  # inMem working db will be in Memory
        dbLoc = ":memory:"
    else:
        log.info(f"In memory db : {dbLoc}")

    inMemDbconn = memdb.initDB(scriptPath=scriptPath, dbLoc=dbLoc)

    appDb = YTClasses.APPdb(args.dbLoc)
    if appDb.chkDB()[0] == 1:
        log.warning("Initializing database")
        appDb.initDB(scriptPath=scriptPath)

    log.info("Connected to database")

    # movie metadata file (json) -> working memDB
    json2memDb(inMemDbconn, appDb)
    # Determine seasons to be updated
    log.info("--- Determining episode numbers ---")
    seasons2Update = memdb.getSeasons2Update(inMemDbconn)
    log.debug(f"seasons to update: {len(seasons2Update)}")
    if len(seasons2Update) == 0:
        log.info("No seasons to update")
    else:
        sCount = 1
        for sRow in seasons2Update:  # Updating each season
            # Get vidID's that need to be updates
            vids2Update = memdb.getVidRecsSeason(inMemDbconn, sRow[0])
            log.debug(
                f"season {sRow[0]} - videos to update {len(vids2Update)}")

            # Get last episode for season
            lastSeasonEpisode = appDb.getLastEpisode(season=sRow[0])

            # Update inMem database with episode numbers
            vCount = 1
            for vidRow in vids2Update:
                vidRowData = memdb.getVidRow(inMemDbconn, vidRow[0])
                curVidRec = vidRow2VidRec(vidRowData)
                lastSeasonEpisode += 1
                curVidRec.episode = lastSeasonEpisode
                curVidRec.dl_file = vidRowData['dl_Filename']
                # Update inmem db record
                memdb.updateVidRec(inMemDbconn, curVidRec)
                log.info(
                    f"Season {curVidRec.season} ({sCount} of {len(seasons2Update)}) Video ({vCount} of {len(vids2Update)}) vid_ID: {curVidRec.vid_ID} assigned episode {curVidRec.episode}")
                vCount += 1

            sCount += 1

        # Begin - Put files in out directory
        createFiles(inMemDbconn, appDb)
        # END process of put files in out directory


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="YouTube file download organizer for plex", epilog="This takes files download from youtube-dl --write-info-json option and will update database, and move vid and metata files so plex scanners can be used.")
    parser.add_argument(
        "--database", help="database file", type=str, required=True, dest="dbLoc", metavar="filename")
    parser.add_argument("-i", "--inFolder", help="Folder/Directory location where vids and json files are",
                        metavar="folderName", type=str, dest="inFolder", required=True)
    parser.add_argument("-o", "--outFolder", help="Folder/Directory location where vids and metadata should be written to",
                        metavar="folderName", type=str, dest="outFolder", required=True)
    parser.add_argument("-l", "--logFile", help="File to Log to",
                        metavar="LogFile", type=str, dest="logFile")
    parser.add_argument(
        "-c", "--copy", help="Copy Video files and do not delete json files", action='store_true', dest="copyOnly")
    parser.add_argument("--noInMemDb", help="Disable inMemory working table",
                        action='store_true', dest="noInMemDb")
    args = parser.parse_args()
    main(args)
