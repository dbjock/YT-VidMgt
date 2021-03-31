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


# App Custome modules
from YTVidMgmt import YTClasses

APP_VER = "1.0"

# Log Formatters
smlFMT = logging.Formatter(
    '%(asctime)s %(levelname)-8s %(message)s')
extFMT = logging.Formatter(
    '%(asctime)s %(levelname)-8s:%(name)s.%(funcName)s: %(message)s')
# Log Handlers
console = logging.StreamHandler()
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


def json2VidRec(jsonFile, delFile=False):
    """Creates vidRec object from jsonFile

    Args:
        jsonFile (Path obj): json file to load
        delFile (bool, optional): Delete jsonFile when converted. Defaults to False.

    Returns:
        VidRec obj: video record class object
    """
    vidRec = YTClasses.VidRec(0)
    # confirm file exists
    if not jsonFile.exists():
        log.warning(f"{jsonFile} does not exist")
        return vidRec

    # json load into dictionary
    with open(jsonFile) as jFile:
        jData = json.load(jFile)
        log.debug(f"Loaded file: {jsonFile}")
        vidRec.vid_ID = jData['id']
        vidRec.vid_url = jData['webpage_url']
        vidRec.channel_url = jData['channel_url']
        # Convert the YYYYMMDD to YYYY-MM-DD for vidRec
        uploadDate = datetime.strptime(jData['upload_date'], '%Y%m%d')
        vidRec.upload_date = uploadDate.strftime('%Y-%m-%d')
        vidRec.description = None
        vidRec.vid_title = jData['title']
        vidRec.dl_file = jData['_filename']

    return vidRec


def createMeta(vidRec, metafName):
    """Creates metafName based on vidRec object
     file format criteria https://bitbucket.org/mjarends/extendedpersonalmedia-agent.bundle/src/0982485ee6d54b5b927434210bd694f29a159ef7/Samples/show.metadata

    Args:
        vidRec (VidRec object class): Video record object
        metafName (str): Full Path and filename of meta file to create
    """
    # Determine output metafile name
    log.debug(f"metafName={metafName}")

    log.debug(f"creating {metafName}")
    with open(metafName, 'w') as oFile:
        oFile.write("[metadata]\n")
        oFile.write(f"title={vidRec.vid_title}\n")
        oFile.write(f"release={vidRec.upload_date}\n")
        # Do not put the summary as this can contain bad data.

    log.info(f"created {metafName}")


def cleanStr(dirtyStr):
    """Replaces $%&*:@'\/" in a string with an underscore

    Args:
        dirtyStr (str): The string to clean

    Returns:
        str: the string cleaned
    """
    badChar = ["$", "!", "%", "&", "*", ":", "@", "'", "\\", "/"]
    log.debug(f"Cleaning {dirtyStr}")
    for b in badChar:
        dirtyStr = dirtyStr.replace(b, "_")

    log.debug(f"Cleaned {dirtyStr}")
    return dirtyStr


def calcFilename(vidRec, YTChannel):
    """Creates a base filename based on factors in the vidRec"""
    # YouTubeChannel_SnnnnEnnn_yyyy-mm-dd_title.id.extension
    # Get a clean title
    cleanTitle = cleanStr(vidRec.vid_title)
    log.debug(f"cleanTitle={cleanTitle}")
    season = vidRec.upload_date[:4]
    log.debug(f"season={season}")
    episode = str(vidRec.episode).zfill(3)
    log.debug(f"episode={episode}")
    upload_date = vidRec.upload_date
    log.debug(f"upload_date={upload_date}")
    vidID = vidRec.vid_ID
    log.debug(f"vidID={vidID}")
    log.debug(f"video file={vidRec.dl_file}")
    return f"{YTChannel} - S{season}E{episode} - {cleanTitle}.{vidID}"


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
    log.info(f"Connecting to database: {args.dbLoc}")
    appDb = YTClasses.APPdb(args.dbLoc)
    if appDb.chkDB()[0] == 1:
        log.warning("Initializing database")
        appDb.initDB(scriptPath=scriptPath)

    log.info("Connected to database")

    # Create list of all json files
    jsonFiles = []
    srcFolder = Path(args.inFolder)
    files = srcFolder.rglob('*.json')
    for x in files:
        jsonFiles.append(x)

    log.info(f"Movies found: {len(jsonFiles)}")
    for jsonFile in jsonFiles:
        # json file -> video record object
        log.info(f"Loading {jsonFile}")
        curVidRec = json2VidRec(jsonFile)

        # Check db to see if video record object id exists
        log.debug(
            f"Checking db for {curVidRec.vid_title} ({curVidRec.vid_ID})")
        dbVidRec = appDb.getVid(curVidRec.vid_ID)
        if dbVidRec.vid_ID == curVidRec.vid_ID:  # exists in db
            log.warning(
                f"{curVidRec.vid_title} ({curVidRec.vid_ID}) exists in db. json data will be ignored")
            log.debug("replace memory vidrec object with db vidrec object")
            # save the download video file name
            dbVidRec.dl_file = curVidRec.dl_file
            # Replace cur video obj with db one
            del curVidRec
            curVidRec = copy.copy(dbVidRec)
            del dbVidRec
        else:  # does not exist in db
            log.debug(
                f"{curVidRec.vid_title} ({curVidRec.vid_ID}) does not exist in db")
            curVidRec.episode = appDb.getLastEpisode() + 1
            # Adding to database
            result = appDb.addVidRec(curVidRec)
            if result[0] != 0:
                # Failure adding
                log.critical(
                    f"Unable to save video record. vid_id: {curVidRec.vid_ID}, vidFile: {vidRec.dl_file}")
                log.critical(f"Return Code: {result}")
                sys.exit(1)

        # Get baseFilename
        baseFilename = calcFilename(curVidRec, Path(args.inFolder).name)
        log.debug(f"Base Filename = {baseFilename}")

        # Set origin and destination directories
        originDir = Path(curVidRec.dl_file).parent
        log.debug(f"originDir = {originDir}")
        destDir = Path(args.outFolder)
        log.debug(f"destDir = {destDir}")

        # Create destDir if it doesnt exist
        log.debug(f"checking if destDir={destDir} exists")
        if not destDir.exists():
            log.warning(f"Creating {destDir}")
            destDir.mkdir(parents=True, exist_ok=True)

        # Set destination metafilename
        xFilename = baseFilename + ".metadata"
        destMetaFileName = destDir / Path(xFilename)
        log.debug(f"metadata file={destMetaFileName}")

        # Set destination Video file name
        xFilename = baseFilename + Path(curVidRec.dl_file).suffix
        destVidFileName = destDir / xFilename
        log.debug(f"video file name={destVidFileName}")

        # If video file is missing then don't create anything
        srcVidFileName = Path(curVidRec.dl_file)
        if srcVidFileName.exists:
            # Create destination metafile
            createMeta(curVidRec, destMetaFileName)
            # Create destination video file
            shutil.copy2(src=srcVidFileName, dst=destVidFileName)
            log.info(f"Created {destVidFileName}")
        else:  # video file does not exist
            log.warning(f"Video file {srcVidFileName} missing")


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
    args = parser.parse_args()
    main(args)