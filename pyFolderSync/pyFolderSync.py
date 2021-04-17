#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import pathlib
import sqlite3
import os
import re
import urllib
import pathlib
import time
import shutil
import hashlib

from pathlib import Path
from datetime import datetime
from sqlite3 import Error
from os import popen


# ================================================================
#
# Module scope variables.
#
# ================================================================


EXT_PATH = '\\\\?\\' if os.name == 'nt' else ''


# ================================================================
#
# Module scope functions
#
# ================================================================


def get_current_folder():
    return str(pathlib.Path(__file__).parent.absolute())


def formate_date_iso8601(date):
    return date.strftime('%Y-%m-%dT%H:%M:%S.%f%z')


def _get_filename(filepath):
    return str(Path(filepath).name)


def _get_parent(filepath):
    return str(Path(filepath).parent)


def get_file_id(filepath):
    return popen('fsutil file queryfileid "{}"'.format(filepath)).read()


def get_descedents(dirIn):
    for root, dirs, files in os.walk(dirIn):
        for name in files:
            yield os.path.join(root, name)
        for name in dirs:
            yield os.path.join(root, name)


# ================================================================
#
# Module scope classes
#
# ================================================================


# Models
# ================================================================

class Sync:

    def __init__(self, folderIn, folderOut):
        self._folderIn = folderIn
        self._folderOut = folderOut

    @staticmethod
    def build_from_dict(dictInput):
        return Sync(dictInput['folderIn'], dictInput['folderOut'])

    def get_folderIn(self):
        return self._folderIn

    def get_folderOut(self):
        return self._folderIn


class Location:

    def __init__(self, sync, folderInLocation, folderInId=None):
        self._sync = sync
        self._folderInLocation = folderInLocation
        self._folderInId = folderInId

    @staticmethod
    def build_from_dict(dictInput):
        return Location(Sync.build_from_dict(dictInput),
                        dictInput['folderInLocation'],
                        dictInput['folderInId'])

    def get_sync(self):
        return self._sync

    def get_folderInLocation(self):
        return self._folderInLocation

    def get_folderInId(self):
        return self._folderInId


# Helper Classes
# ================================================================

class DatabaseConnector:

    def __init__(self, dataFolder=get_current_folder(), dbSetupFolder=get_current_folder()):
        self.setupFileLoc = dbSetupFolder + "/tableSetup.sql"
        self.conn = self._create_connection(dataFolder)
        self.conn.row_factory = sqlite3.Row
        self._run_setup()

    def _create_connection(self, db_path):
        """ create db conn """
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        return sqlite3.connect(db_path + "/sqllite.db", check_same_thread=False)

    def _run_setup(self):
        """ sets up database tables """
        cursor = self.conn.cursor()
        sql_file = open(self.setupFileLoc)
        sql_as_string = sql_file.read()
        cursor.executescript(sql_as_string)

    def execute(self, query, args):
        """Executes sql statements, and maps response to objects"""
        cursor = self.conn.cursor()
        cursor.execute(query, args)
        self.conn.commit()
        dictList = [dict(row) for row in cursor.fetchall()]
        return dictList

    def executeBatch(self, query, argsList):
        """Executes sql statements, and maps response to objects"""
        cursor = self.conn.cursor()
        for arg in argsList:
            cursor.execute(query, arg)
        self.conn.commit()
        dictList = [dict(row) for row in cursor.fetchall()]
        return dictList


class DataStore:

    SYNC_TB = "sync"
    CREATE_SYNC = "INSERT OR IGNORE INTO {} (folderIn, folderOut) VALUES (?,?);".format(SYNC_TB)

    LOC_TB = "location"
    CREATE_LOC = "INSERT INTO {} (folderIn, folderOut, folderInLocation, folderInHash, folderInDateCreated) VALUES (?,?,?,?,?);".format(LOC_TB)
    READ_LOC = "SELECT * FROM {} WHERE folderIn = ? folderOut = ? folderId = ?;".format(LOC_TB)
    UPDATE_LOC = "UPDATE {} SET folderInLocation = ? WHERE folderIn = ? AND folderOut = ? AND folderInLocation = ?;".format(LOC_TB)
    REMOVE_LOC = "DELETE FROM {} WHERE folderIn = ? AND folderOut = ? AND folderInLocation = ?;".format(LOC_TB)

    def __init__(self, dbConn):
        self.dbConn = dbConn

    def _records_to_locations(self, records):
        locations = []
        for record in records:
            locations.append(Location.build_from_dict(record))
        return locations

    # SYNC

    def create_sync(self, sync):
        args = (sync.get_folderIn(), sync.get_folderOut())
        self.dbConn.execute(DataStore.CREATE_SYNC, args)

    # LOCATION

    def create_location(self, loc):
        sync = loc.get_sync()
        args = (sync.get_folderIn(),
                sync.get_folderOut(),
                loc.get_folderInLocation(),
                loc.get_folderInId())
        self.dbConn.execute(DataStore.CREATE_LOC, args)

    def read_location(self, sync, folderId):
        args = (sync.get_folderIn(),
                sync.get_folderOut(),
                folderId)
        records = self.dbConn.execute(DataStore.READ_LOC, args)
        locations = self._records_to_locations(records)
        return locations[0] if locations else None

    def update_location(self, oldLoc, newloc):
        sync = newloc.get_sync()
        args = (newloc.get_folderInLocation(),
                sync.get_folderIn(),
                sync.get_folderOut(),
                oldLoc.get_folderInLocation())
        self.dbConn.execute(DataStore.UPDATE_LOC, args)

    def remove_location(self, loc):
        sync = loc.get_sync()
        args = (sync.get_folderIn(),
                sync.get_folderOut(),
                loc.get_folderInLocation())
        self.dbConn.execute(DataStore.REMOVE_LOC, args)

# Primary Class
# ================================================================


class FolderSync:
    """
    Keeps the folderOut in sync with the folderIn.
    Frequency (seconds) is the sleep time after run.
    Moves are tracked by fileid (via fsutil file queryfileid)
    """

    def __init__(self,
                 folderIn="F:/test stuff/syncTest/in",
                 #folderOut="F:/test stuff/syncTest/out",
                 folderOut="\\\\DESKTOP-UPOMV61\\Users\\lucas\Desktop\\test",
                 frequency=2):

        # set vals
        self.folderIn = folderIn
        self.folderOut = folderOut
        self.sync = Sync(self.folderIn, self.folderOut)
        self.frequency = frequency
        self.dataStore = DataStore(DatabaseConnector())

    def run(self):
        """
        Steps:
        1. iterate folderIn
            for each file/folder:
                1.5 File actions:
                - if in folderIn, not in folderOut, -> create
                - if in folderIn, in folderOut, if last modified > folderOut last modified -> update
        2. iterate folderOut locations
            for each location:
                2.5 folderOut actions:
                - if in folderOut, not in folderIn, -> delete
        """
        # make sync
        self.dataStore.create_sync(self.sync)

        # run forever
        while True:

            self.get_descedents(self.folderIn, self.handle_inFile)
            self.get_descedents(self.folderOut, self.handle_outFile)

            time.sleep(self.frequency)

    def get_descedents(self, dirIn, function):
        for root, dirs, files in os.walk(dirIn):
            for name in files:
                function(os.path.join(root, name))
            for name in dirs:
                function(os.path.join(root, name))

    def handle_inFile(self, inFilepath):
        # build vars
        outFilepath = self._build_sync_filepath(self.folderIn, self.folderOut, inFilepath)
        
        # update filestats based on last modified time
        if os.path.exists(inFilepath) and os.path.exists(outFilepath):
            location = Location(self.sync, inFilepath, get_file_id(inFilepath))
            if os.stat(inFilepath).st_mtime != os.stat(outFilepath).st_mtime:
                if os.path.isdir(outFilepath):
                    shutil.copystat(inFilepath, outFilepath)
                else:
                    shutil.copy2(inFilepath, outFilepath)
        # create or (updated name)
        if not(os.path.exists(outFilepath)):
            # also need to handle the 'updated' case here as well, as to not create any new files
            # delete OR updated names (based on dir hash and creation time to see if it exists already)
            # if db has more than one hash == in the same sync, then ignore
            # https://stackoverflow.com/questions/24937495/how-can-i-calculate-a-hash-for-a-filesystem-directory-using-python
            # os.rename(outFilepath, _get_parent(outFilepath) + '/' + _get_filename(inFilepath))
            location = Location(self.sync, inFilepath, get_file_id(inFilepath))
            # modify
            priorLocation = self.dataStore.read_location(self.sync, get_file_id(inFilepath))
            if priorLocation:
                # map old inFileLocation to old outFileLocation
                oldOutfile = self._build_sync_filepath(self.folderIn, self.folderOut, priorLocation.get_folderInLocation())
                # move old outFile to new outfile
                shutil.move(oldOutfile, outFilepath)
                # track move in db
                oldLocation = Location(self.sync, self._build_sync_filepath(self.folderOut, self.folderIn, oldOutfile))
                self.dataStore.update_location(oldLocation, location)
                if os.path.isdir(inFilepath):
                    for oldfileLoc in get_descedents(inFilepath):
                        oldLocation = Location(self.sync, self._build_sync_filepath(self.folderOut, self.folderIn, oldfileLoc))
                        self.dataStore.update_location(oldLocation, location)

            # create
            if os.path.isdir(inFilepath):
                shutil.copytree(inFilepath, outFilepath)
                # track create in db
                self.dataStore.create_location(location)
                for fileLoc in get_descedents(inFilepath):
                    self.dataStore.create_location(Location(self.sync, fileLoc, get_file_id(fileLoc)))
            else:
                shutil.copy2(inFilepath, outFilepath)
                # track create in db
                self.dataStore.create_location(location)

    def handle_outFile(self, outFilepath):
        # build in
        inFilepath = self._build_sync_filepath(self.folderOut, self.folderIn, outFilepath)

        if not(os.path.exists(inFilepath)) and os.path.exists(outFilepath):
            oldLocation = Location(self.sync, self._build_sync_filepath(self.folderOut, self.folderIn, outFilepath))
            if os.path.isdir(outFilepath):
                shutil.rmtree(outFilepath)
                self.dataStore.remove_location(oldLocation)
                for oldfileLoc in get_descedents(inFilepath):
                    oldLocation = Location(self.sync, self._build_sync_filepath(self.folderOut, self.folderIn, oldfileLoc))
                    self.dataStore.remove_location(oldLocation)
            else:
                os.remove(outFilepath)
                self.dataStore.remove_location(oldLocation)

    def _build_sync_filepath(self, rootDirIn, rootDirOut, filepathIn):
        """ takes filepath, and re-builds it under rootDirOut """
        relativePathIn = filepathIn.split(rootDirIn, 1)[1]
        filepathOut = rootDirOut + relativePathIn
        return filepathOut

FolderSync().run()