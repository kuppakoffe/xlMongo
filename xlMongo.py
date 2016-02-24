#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import argparse
import requests
import traceback
import threading
import unicodecsv
from sets import Set
from yaml import load
from uuid import uuid4
from bson import ObjectId
from pymongo import errors
from googleAuth import GDrive
from datetime import datetime
from pymongo import MongoClient


class MongoUtils(object):
    """
    This will perform some basic actions on mongodb
    like getting the list of all databases, getting
    all the collections in a particular database,
    getting the indexes on each collection and maybe
    creating an index at some point in time!
    """

    def __init__(self, **kwargs):
        """
        A constructor function requiring the following params:
        parameters:
        - `hostname`: hostname where mongod is running
        - `port`: port on which mongod is listening
        """
        self._hostname = kwargs['hostname']
        self._port = kwargs['port']
        self._connection = None

    def getConnection(self):
        ret = False
        try:
            self._connection = MongoClient(
                host=self._hostname,
                port=self._port)
            if self._connection.server_info():
                ret = True
        except (errors.ServerSelectionTimeoutError) as e:
            print ("Server Connection Time Out")
            sys.exit()
        except (Exception) as e:
            print(e.message)
            print(traceback.format_exc())
            sys.exit()
        return ret

    def scanDatabases(self):
        ret = None
        try:
            return self._connection.database_names()
        except (Exception) as e:
            print(e.message)
            print(traceback.format_exc())
        return ret

    def getDBConnection(self, dbName):
        return self._connection.get_database(dbName)

    def scanCollections(self,
                        databases=[],
                        ignoredDatabases=['local', 'config', 'test', 'admin'],
                        system_collections=False):
        collectionDict = {}
        try:
            for dbname in databases:
                if dbname in ignoredDatabases:
                    continue
                db = self._connection.get_database(dbname)
                collectionDict[dbname] = db.collection_names(
                    include_system_collections=system_collections)
        except Exception as e:
            print(e.message)
            print(traceback.format_exc())
        return collectionDict

    def getCollectionData(self, cursor, lock, collDict):
        """
        This method will use muultiple threads
        to fetch the data from collection and will
        update the outList with the output
        """
        name = threading.current_thread().name
        print("Starting %s" % name)
        collDict[name] = list()
        try:
            lock.acquire()
            collDict[name] = [doc for doc in cursor]
            lock.release()
        except StopIteration:
            pass


class Author(object):
    """
    This class will just return some basic information
    about the author of the document, like :
    `author name` : Will be the user logged into terminal
    `date`: The date when the documents are altered
    `runId`: Well you need to keed track of changes somehow
    `ip`: It's good to know from where you made what changes :)
    """

    def getName(self):
        """
        Returns the logged in username
        """
        return os.environ['USER']

    def getDate(self):
        """
        Returns the datetime in specified format
        """
        return datetime.strftime(
            datetime.now(), "%d/%m/%Y %H:%M:%S")

    def getRunId(self):
        """
        Returns a unique hashcode for tracking changes
        """
        return uuid4().hex

    def getIp(self):
        """
        Return the current ip address of the logged in user.
        Will check for few web based api and will break if gets
        response from any 1 of them
        """
        ip = None
        sites = [
            'http://jsonip.com',
            'http://httpbin.org/ip',
            'https://api.ipify.org/?format=json']
        for site in sites:
            try:
                get = requests.get(site)
                if get.status_code == 200:
                    ret = get.json()
                    if 'ip' in ret:
                        ip = ret['ip']
                        break
                    if 'origin' in ret:
                        ip = ret['origin']
                        break
            except Exception:
                pass
        return ip


def compareKeys(jsonList):
    """
    Will compare all the objects inside jsonList and
    will return all the keys in form of a list
    """
    allKeys = list()
    jsonList = jsonList
    limit = len(jsonList)
    if limit == 1:
        allKeys = jsonList[0].keys()
        return allKeys
    allKeysSet = Set()
    [allKeysSet.update(obj.keys()) for obj in jsonList]
    return list(allKeysSet)


def jsonToCsv(name, headerList, dictList, outDirectory):
    """
    Will take a json / dict object and will convert
    it to csv format, the out directory path will
    be needed in order to save the file
    """
    fileName = '%s.csv' % name
    fileName = os.path.join(outDirectory, fileName)
    if not os.path.isdir(outDirectory):
        os.mkdir(outDirectory)
    with open(fileName, 'w') as fp:
        csvwriter = unicodecsv.writer(fp)
        csvwriter.writerow(headerList)
        for element in dictList:
            dictObject = element
            row = list()
            for key in headerList:
                if key not in dictObject:
                    val = 'NULL'
                else:
                    val = dictObject[key]
                row.append(val)
            try:
                csvwriter.writerow(row)
            except Exception:
                # print ("Error at %s" % row)
                print (row)
                print (traceback.format_exc())
                sys.exit()


def run(confiFile):
    firstRun = False
    with open(configFile, 'r') as fp:
        data = load(fp.read())
    appName = data['APP']['applicationname']
    baseDir = os.path.abspath(data['APP']['basedirectory'])
    drive = GDrive(
        scopes=data['DRIVE']['scopes'],
        client_secret_file=data['DRIVE']['client_secret_file'],
        applicationName=appName
    )
    uploadDirectory = os.path.join(baseDir, 'upload')
    downloadDirectory = os.path.join(baseDir, 'download')
    baseDirectory = os.path.join(baseDir, 'base')
    if not os.path.isdir(uploadDirectory):
        os.mkdir(uploadDirectory)
    if not os.path.isdir(downloadDirectory):
        os.mkdir(downloadDirectory)
    if not os.path.isdir(baseDirectory):
        os.mkdir(baseDirectory)

    if drive.getCredentials():
        folderId = drive.getFolderId(appName)
        print (folderId)
        if not folderId:
            print (
                "Hmm First run, hang in there , job will be done in no time!"
            )
            folderId = drive.createFolder(appName)
            firstRun = True
    else:
        print("Some issue in getting connected to drive , Exiting!")
        sys.exit()
    # ==== Mongo Related variables ======
    mongo_databases = None
    if data['DATABASE']['type'] == 'mongo':
        mongo_host = data['DATABASE']['host']
        mongo_port = data['DATABASE']['port']
        if 'databases' in data['DATABASE']:
            mongo_databases = data['DATABASE']['databases']
        mongoutil = MongoUtils(
            hostname=mongo_host,
            port=mongo_port)
        if mongoutil.getConnection():
            if not mongo_databases:
                print ("Getting all the databases")
                mongo_databases = mongoutil.scanDatabases()
            collections = mongoutil.scanCollections(databases=mongo_databases)
            for db, collectionList in collections.items():
                dbConnection = mongoutil.getDBConnection(db)
                collectionList = [dbConnection.get_collection(
                    name) for name in collectionList]
            THREADS = []
            collDict = {}
            for coll in collectionList:
                name = "%s.%s" % (coll.database.name, coll.name)
                lock = threading.Lock()
                # cursors = coll.parallel_scan(4)
                cursors = [coll.find()]
                for cursor in cursors:
                    thread = threading.Thread(
                        target=mongoutil.getCollectionData,
                        args=(cursor, lock, collDict),
                        name=name)
                THREADS.append(thread)
            for thread in THREADS:
                thread.start()
            for thread in THREADS:
                thread.join()
            headings = {}
            """
            for key, val in collDict.items():
                print ("%s : %s" % (key, val))
            """
            # print (collDict)
            THREADS = []
            for key in collDict:
                headings[key] = compareKeys(collDict[key])
            # print (json.dumps(headings, indent=3, sort_keys=True))
            # name, headerList, dictList, outDirectory
            for key, val in collDict.items():
                print ("%s has %s docs" % (key, len(val)))
                headerList = headings[key]
                dictList = collDict[key]
                outDirectory = uploadDirectory
                thread = threading.Thread(target=jsonToCsv, args=(
                    key,
                    headerList,
                    dictList,
                    outDirectory), name=key)
                THREADS.append(thread)
            for thread in THREADS:
                thread.start()
            for thread in THREADS:
                thread.join()
            if firstRun:
                for fileName in os.listdir(uploadDirectory):
                    filePath = os.path.join(uploadDirectory, fileName)
                    id = drive.createFile(filePath, folderId)
                    print (id)
if __name__ == '__main__':
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c',
        '--configFile',
        help='Configuration File [optional]',
        type=str
    )
    parser.add_argument(
        '-o',
        '--option',
        help='option to execute',
        type=str
    )
    args = parser.parse_args()
    configFile = args.configFile
    option = args.option
    if not configFile:
        configFile = os.path.join(
            os.path.abspath('.'),
            'config.yaml'
        )
    if not option:
        print (
            "I Obey. Dalek needs a command... Explain! Run with -h for help")
        sys.exit()
    optionList = ['execute', 'makemigrations', 'runmigration']
    if option not in optionList:
        print (
            "Invalid! With commands below the Dalek will be  twice as ... useful:\n*%s" % (
                "\n*".join(optionList)))
        sys.exit()
    print ("ConfigFile: %s" % configFile)
    if option == 'execute':
        run(configFile)
    if len(sys.argv) < 2:
        print (
            "I Obey. Dalek needs a command... Explain! Run with -h for help")
        sys.exit()
    command = sys.argv[1]
    optionList = ['execute', 'makemigrations', 'runmigration']
    if command not in optionList:
        print (
            "Invalid! With commands below the Dalek will be  twice as ... useful:\n*%s" % (
                "\n*".join(optionList)))
        sys.exit()
    """
    configFile = 'config.yaml'
    run(configFile)


def test():
    drive = GDrive(
        scopes=[
            'https://www.googleapis.com/auth/drive.file',
            'https://spreadsheets.google.com/feeds'],
        client_secret_file='client_secret.json',
        applicationName='pConnect')
    drive.getCredentials()
    files = drive.getFileObject()
    # print (drive.fileList(files))
    file_metadata = {'mimeType': 'application/vnd.google-apps.spreadsheet',
                     'description': 'Just checking', 'title': 'sampleUpload',
                     'name': 'pconnect.schools'}
    media_body = MediaFileUpload(
        filename="/tmp/outDir/pconnect.schools.csv",
        mimetype='text/csv',
        resumable=True)
    """
    file = files.create(body=file_metadata, media_body=media_body,
                    fields='id').execute()
    print (file.get('id'))
    """
