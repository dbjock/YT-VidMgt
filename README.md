
# YT-VidMgmt

YT-VidMgmt creates/tracks Season and Episode numbers from the json files created from youtube-dl so naming conventions are consistent for use with mjarends Plex [personal media scanner](https://bitbucket.org/mjarends/plex-scanners) and [personal media agent](https://bitbucket.org/mjarends/extendedpersonalmedia-agent.bundle).

## Requirements

- Python version 3.8

## Usage

```
Tested with Python 3.8

YouTube file download organizer for plex

optional arguments:
  -h, --help            show this help message and exit
  --database filename   database file
  -i folderName, --inFolder folderName
                        Folder/Directory location where vids and json files are
  -o folderName, --outFolder folderName
                        Folder/Directory location where vids and metadata should be written to
  -l LogFile, --logFile LogFile
                        File to Log to
  -c, --copy            Testing. Video files will be copied not moved.
  --noInMemDb           Disable inMemory working table

This takes files download from youtube-dl --write-info-json option and will update database, and move vid and metata files so plex scanners can be used.
```

## Change Log

Version 1.21
- Console logging output switch to sys.stdout
Version 1.2
- Non ascii characters are stripped from video title.
- FIX - youtube-dl json files are deleted. (-c they will not be)

## Using with youtube-dl

TODO: Add more stuff here as I work this out.