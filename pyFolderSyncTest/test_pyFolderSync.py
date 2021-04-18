#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import os
import inspect
import shutil
import json

from pathlib import Path
from pyFolderSync import pyFolderSync


# Utilities
# =================================================================

# File
# ====================


def _read_file(path, mode='rb'):
    with open(path, mode) as ifp:
        return ifp.read()


def _write_file(path, content, mode='wb'):
    with open(path, mode) as ofp:
        ofp.write(content)


def _get_filename(filepath):
    return str(Path(filepath).name)


def _get_parent(filepath):
    return str(Path(filepath).parent)

# serialization
# ====================


def jsonToFiles(jsonInp, path):
    jsonInp = jsonInp if isinstance(jsonInp, list) else [jsonInp]
    for filObj in jsonInp:
        if 'folder' in filObj:
            folder = filObj['folder']
            os.makedirs(path + folder['path'])
            jsonToFiles(folder['children'], path + folder['path'])
        if 'file' in filObj:
            filee = filObj['file']
            _write_file(path + filee['path'],
                        filee['contents'],
                        "w")


def filesToJson(path):
    if os.path.isdir(path):
        children = os.listdir(path)
        childObjs = []
        for child in children:
            childObjs.append(filesToJson(os.path.join(path, child)))
        return {
            'folder': {
                'path': '/' + _get_filename(path),
                'children': childObjs
            }
        }
    else:
        return {
            'file': {
                'path': '/' + _get_filename(path),
                'contents': _read_file(path, "r+")
            }
        }

# Tests
# =================================================================


class TestPyFolderSync(unittest.TestCase):

    WORKING_DIR = _get_parent(inspect.getfile(inspect.currentframe()))
    RESOURCES_DIR = WORKING_DIR + '\\resources'
    FOLDER_TREE = json.load(open(RESOURCES_DIR + '\\base-folder-tree.json',))

    TEST_WORKING_FOLDER = os.path.join(WORKING_DIR, 'TEST_WORKING_FOLDER')

    TEST_IN_FOLDER = TEST_WORKING_FOLDER + '\\in'
    TEST_OUT_FOLDER = TEST_WORKING_FOLDER + '\\out'

    TEST_IN_FOLDER_ROOT = TEST_IN_FOLDER + '\\root'
    TEST_OUT_FOLDER_ROOT = TEST_OUT_FOLDER + '\\root'

    def setUp(self):
        self.tearDown()
        os.makedirs(TestPyFolderSync.TEST_IN_FOLDER)
        os.makedirs(TestPyFolderSync.TEST_OUT_FOLDER)
        jsonToFiles(TestPyFolderSync.FOLDER_TREE, TestPyFolderSync.TEST_IN_FOLDER)
        self.maxDiff = 2000

    def tearDown(self):
        # destroy any possible files
        if os.path.exists(TestPyFolderSync.TEST_WORKING_FOLDER):
            shutil.rmtree(TestPyFolderSync.TEST_WORKING_FOLDER)

    def test_simple_sync_files(self):
        # sync
        pyFolderSync.FolderSync(TestPyFolderSync.TEST_IN_FOLDER,
                                TestPyFolderSync.TEST_OUT_FOLDER,
                                frequency=None).run()
        # assert equals
        jsonStringIN = json.dumps(filesToJson(TestPyFolderSync.TEST_IN_FOLDER_ROOT))
        jsonStringOut = json.dumps(filesToJson(TestPyFolderSync.TEST_OUT_FOLDER_ROOT))
        self.assertEqual(jsonStringIN, jsonStringOut)

    def test_create_files(self):
        # sync
        pyFolderSync.FolderSync(TestPyFolderSync.TEST_IN_FOLDER,
                                TestPyFolderSync.TEST_OUT_FOLDER,
                                frequency=None).run()
        # make changes
        os.makedirs(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\testFolder1')
        os.makedirs(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\testFolder2')
        os.makedirs(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\testFolder2\\YELLO')
        os.makedirs(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\New York\\YELLO')

        _write_file(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\testFolder1\\hello.txt', "I am mister winner!!", "w")
        _write_file(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\New York\\YELLO\\NOPE.txt', "I am miaaaa!!", "w")
        _write_file(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\hello.txt', "I am mister winner!!", "w")

        # sync
        pyFolderSync.FolderSync(TestPyFolderSync.TEST_IN_FOLDER,
                                TestPyFolderSync.TEST_OUT_FOLDER,
                                frequency=None).run()

        # assert equals
        jsonStringIN = json.dumps(filesToJson(TestPyFolderSync.TEST_IN_FOLDER_ROOT))
        jsonStringOut = json.dumps(filesToJson(TestPyFolderSync.TEST_OUT_FOLDER_ROOT))
        self.assertEqual(jsonStringIN, jsonStringOut)

    def test_modify_files(self):
        # sync
        pyFolderSync.FolderSync(TestPyFolderSync.TEST_IN_FOLDER,
                                TestPyFolderSync.TEST_OUT_FOLDER,
                                frequency=None).run()
        # make changes
        shutil.move(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\New York',
                    TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\New York2')
        shutil.move(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\New York2\\notes.txt',
                    TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\New York2\\notes3.txt')
        shutil.move(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\photosFun.txt',
                    TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\nope.txt')
        shutil.move(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\testFile2.txt',
                    TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\GOSHHHH.txt')

        _write_file(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\photosFun.txt', "WHAT IS HAPPENING", "w")
        _write_file(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\testFile1.txt', "TEST BOIIII", "w")
        _write_file(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\testFile2.txt', "TEST BOIIIIwerwer", "w")

        # sync
        pyFolderSync.FolderSync(TestPyFolderSync.TEST_IN_FOLDER,
                                TestPyFolderSync.TEST_OUT_FOLDER,
                                frequency=None, deleteWaitlist=False).run()

        # assert equals
        jsonStringIN = json.dumps(filesToJson(TestPyFolderSync.TEST_IN_FOLDER_ROOT))
        jsonStringOut = json.dumps(filesToJson(TestPyFolderSync.TEST_OUT_FOLDER_ROOT))
        self.assertEqual(jsonStringIN, jsonStringOut)
    
    def test_delete_files(self):
        # sync
        pyFolderSync.FolderSync(TestPyFolderSync.TEST_IN_FOLDER,
                                TestPyFolderSync.TEST_OUT_FOLDER,
                                frequency=None).run()
        # make changes
        shutil.rmtree(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\New York')
        os.remove(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\photos\\photosFun.txt')
        os.remove(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\testFile1.txt')
        os.remove(TestPyFolderSync.TEST_IN_FOLDER_ROOT + '\\testFile2.txt')

        # sync
        pyFolderSync.FolderSync(TestPyFolderSync.TEST_IN_FOLDER,
                                TestPyFolderSync.TEST_OUT_FOLDER,
                                frequency=None, deleteWaitlist=False).run()

        # assert equals
        jsonStringIN = json.dumps(filesToJson(TestPyFolderSync.TEST_IN_FOLDER_ROOT))
        jsonStringOut = json.dumps(filesToJson(TestPyFolderSync.TEST_OUT_FOLDER_ROOT))
        self.assertEqual(jsonStringIN, jsonStringOut)

if __name__ == '__main__':
    unittest.main()
