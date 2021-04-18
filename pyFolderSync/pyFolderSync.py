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
import traceback

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


def make_parent_if_not_exists(filepath):
    parentOutFilepath = _get_parent(filepath)
    if not(os.path.isdir(parentOutFilepath)):
        os.makedirs(parentOutFilepath)


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
        return self._folderOut


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
    CREATE_SYNC = """INSERT OR IGNORE INTO {}
                     (folderIn, folderOut) VALUES (?,?);""".format(SYNC_TB)

    LOC_TB = "location"
    CREATE_LOC = """INSERT OR IGNORE INTO {}
                    (folderIn, folderOut, folderInLocation, folderInId)
                    VALUES (?,?,?,?);""".format(LOC_TB)
    READ_LOC = """SELECT * FROM {}
                  WHERE folderIn = ? AND folderOut = ? AND folderInId = ?;""".format(LOC_TB)
    UPDATE_LOC = """UPDATE {} SET
                    folderInLocation = ? WHERE folderIn = ? AND folderOut = ? AND folderInLocation = ?;""".format(LOC_TB)
    REMOVE_LOC = """DELETE FROM {}
                    WHERE folderIn = ? AND folderOut = ? AND folderInLocation = ?;""".format(LOC_TB)

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
    Steps:
        1. iterate folderIn
            for each file/folder:
                1.5 File actions:
                - if in folderIn, not in folderOut, -> create or check if moved (modified path)
                - if in folderIn, in folderOut, if last modified > folderOut last modified -> update
        2. iterate folderOut locations
            for each location:
                2.5 folderOut actions:
                - if in folderOut, not in folderIn, -> delete
    """

    def __init__(self, folderIn, folderOut, frequency=2, deleteWaitlist=True):

        # set vals
        self.deleteWaitlist = deleteWaitlist  # waits one run for deletes to happen (more optimal in case move happens)
        self.folderIn = EXT_PATH + folderIn
        self.folderOut = EXT_PATH + folderOut
        self.sync = Sync(self.folderIn, self.folderOut)
        self.frequency = frequency
        self.dataStore = DataStore(DatabaseConnector())
        self.waitForDelete = set()  # waits until next run to delete

    # Main Loop
    # =================================================================

    def run(self):

        # make sync
        self.dataStore.create_sync(self.sync)

        # run forever
        while True:

            for filee in get_descedents(self.folderIn):
                try:
                    self.handle_inFile(filee)
                except Exception:
                    print("failed to deal with folderIn file:" + filee)
                    traceback.print_exc()

            for filee in get_descedents(self.folderOut):
                try:
                    self.handle_outFile(filee)
                except Exception:
                    print("failed to deal with folderOut file:" + filee)
                    traceback.print_exc()

            if self.frequency:
                time.sleep(self.frequency)
            else:
                break

    # Handlers
    # =================================================================

    # Infile handler
    # ==================================

    def handle_inFile(self, inFilepath):
        """ handles each file in the src directory to decide on creates/updates """
        # build vars
        outFilepath = self._build_sync_filepath(self.folderIn, self.folderOut, inFilepath)

        # update file
        if os.path.exists(inFilepath) and os.path.exists(outFilepath):
            self.update_file(inFilepath, outFilepath)

        # create or move file/files
        if not(os.path.exists(outFilepath)):

            location = Location(self.sync, inFilepath, get_file_id(inFilepath))
            priorLocation = self.dataStore.read_location(self.sync, get_file_id(inFilepath))

            if priorLocation:
                # move file/files
                self.move_file(inFilepath, outFilepath, location, priorLocation)
            else:
                # create file/files
                self.create_file(inFilepath, outFilepath, location)

    # Helpers
    # ==================

    def update_file(self, inFilepath, outFilepath):
        # if modified times don't match, rectify
        if os.stat(inFilepath).st_mtime != os.stat(outFilepath).st_mtime:
            if os.path.isdir(outFilepath):
                shutil.copystat(inFilepath, outFilepath)
            else:
                shutil.copy2(inFilepath, outFilepath)

    def create_file(self, inFilepath, outFilepath, location):
        # make parent if not exists (only should happen if user edits while running)
        make_parent_if_not_exists(outFilepath)
        # make file
        if os.path.isdir(inFilepath):
            # cp
            shutil.copytree(inFilepath, outFilepath)
            # track create in db
            self.dataStore.create_location(location)
            for fileLoc in get_descedents(inFilepath):
                self.dataStore.create_location(Location(self.sync, fileLoc, get_file_id(fileLoc)))
        else:
            # cp
            shutil.copy2(inFilepath, outFilepath)
            # track create in db
            self.dataStore.create_location(location)

    def move_file(self, inFilepath, outFilepath, location, priorLocation):
        # map old inFileLocation to old outFileLocation
        oldOutfile = self._build_sync_filepath(self.folderIn, self.folderOut, priorLocation.get_folderInLocation())
        if os.path.exists(oldOutfile):
            # make parent if not exists (only should happen if user edits while running)
            make_parent_if_not_exists(outFilepath)
            # move old outFile to new outfile
            shutil.move(oldOutfile, outFilepath)
            # track move in db
            oldLocation = Location(self.sync, self._build_sync_filepath(self.folderOut, self.folderIn, oldOutfile))
            self.dataStore.update_location(oldLocation, location)
            if os.path.isdir(inFilepath):
                # track move in all descendents in db
                for newfileLoc in get_descedents(inFilepath):
                    priorLocation = self.dataStore.read_location(self.sync, get_file_id(newfileLoc))
                    if priorLocation:
                        oldOutfile = self._build_sync_filepath(self.folderIn,
                                                               self.folderOut,
                                                               priorLocation.get_folderInLocation())
                        oldLocation = Location(self.sync, self._build_sync_filepath(self.folderOut,
                                                                                    self.folderIn,
                                                                                    oldOutfile))
                        self.dataStore.update_location(oldLocation, location)

    # Outfile handler
    # ==================================

    def handle_outFile(self, outFilepath):
        """ handles each file in the output directory to decide on deletes """
        # build vars
        inFilepath = self._build_sync_filepath(self.folderOut, self.folderIn, outFilepath)
        oldLocation = Location(self.sync, self._build_sync_filepath(self.folderOut, self.folderIn, outFilepath))

        # delete file/files
        if not(os.path.exists(inFilepath)) and os.path.exists(outFilepath):
            self.delete_file(inFilepath, outFilepath, oldLocation)

    # Helpers
    # ==================

    def delete_file(self, inFilepath, outFilepath, oldLocation):
        # only continue if it is in the waitlist (to avoid situation of move after handle_infile)
        if not(self.deleteWaitlist) or outFilepath in self.waitForDelete:
            # rm curr from waitlist
            if self.deleteWaitlist:
                self.waitForDelete.remove(outFilepath)

            if os.path.isdir(outFilepath):
                # rm all descendents from waitlist
                if self.deleteWaitlist:
                    for fileLoc in get_descedents(outFilepath):
                        if fileLoc in self.waitForDelete:
                            self.waitForDelete.remove(fileLoc)
                # rm
                shutil.rmtree(outFilepath)
                # track rm in db
                self.dataStore.remove_location(oldLocation)
                for oldfileLoc in get_descedents(inFilepath):
                    oldLocation = Location(self.sync, self._build_sync_filepath(self.folderOut, self.folderIn, oldfileLoc))
                    self.dataStore.remove_location(oldLocation)
            else:
                # rm
                os.remove(outFilepath)
                # track rm in db
                self.dataStore.remove_location(oldLocation)
        else:
            # add to waitlist
            if self.deleteWaitlist:
                self.waitForDelete.add(outFilepath)

    # Utilities
    # =================================================================

    def _build_sync_filepath(self, rootDirIn, rootDirOut, filepathIn):
        """ takes filepath, and re-builds it under rootDirOut """
        relativePathIn = filepathIn.split(rootDirIn, 1)[1]
        filepathOut = rootDirOut + relativePathIn
        return filepathOut

#FolderSync("F:\\test stuff\\syncTest\\in", "F:\\test stuff\\syncTest\\out").run()