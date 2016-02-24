#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import httplib2
import oauth2client
import traceback
import threading
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from apiclient.http import MediaFileUpload


try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


class GDrive(object):

    def __init__(self, **kwargs):
        self._scopes = kwargs['scopes']
        self._client_secret_file = kwargs['client_secret_file']
        self._applicationName = kwargs['applicationName']
        self._files = None
        self._credentials = None
        self._cache = None

    def getCredentials(self):
        ret = False
        try:
            home_dir = os.path.expanduser('~')
            credential_dir = os.path.join(home_dir, '.credentials')
            if not os.path.exists(credential_dir):
                os.makedirs(credential_dir)
            credential_path = os.path.join(credential_dir,
                                           self._applicationName)
            store = oauth2client.file.Storage(credential_path)
            credentials = store.get()
            if not credentials or credentials.invalid:
                flow = client.flow_from_clientsecrets(
                    self._client_secret_file, self._scopes)
                flow.user_agent = self._applicationName
                if flags:
                    credentials = tools.run_flow(flow, store, flags)
                else:  # Needed only for compatibility with Python 2.6
                    credentials = tools.run(flow, store)
                print('Storing credentials to ' + credential_path)
            self._credentials = credentials
            ret = True
        except Exception:
            print (traceback.format_exc())
        return ret

    def getFileObject(self):
        ret = False
        try:
            http = self._credentials.authorize(httplib2.Http())
            service = discovery.build('drive', 'v3', http=http)
            # print (dir(service))
            # print (service.permissions())
            self._files = service.files()
            ret = True
        except Exception:
            print (traceback.format_exc())
        return ret

    def getFolderId(self, folderName):
        if not self._cache:
            if not self._files:
                self.getFileObject()
            self.fileList()
        for file in self._cache:
            if file['name'] == folderName and file['mimeType'] == 'application/vnd.google-apps.folder':
                return file['id']
        return None

    def createFolder(self, folderName):
        ret = None
        try:
            file_metadata = {
                'name': folderName,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            res = self._files.create(
                body=file_metadata, fields='id').execute()
            ret = res.get('id')
            print ("Folder created with id: %s" % ret)
        except Exception:
            print (traceback.format_exc())
        return ret

    def updateFile(self, fileId):
        pass

    def createFile(self, filePath, parentFolderId):
        try:
            threadName = threading.current_thread().name
            print ("Running: %s" % threadName)
            name = os.path.basename(filePath).split('.csv')[0]
            file_metadata = {
                'mimeType': 'application/vnd.google-apps.spreadsheet',
                'name': name,
                'parents': [parentFolderId]
            }
            media_body = MediaFileUpload(
                filename=filePath,
                mimetype='text/csv',
                resumable=True)
            if not self._files:
                self.getFileObject()
            res = self._files.create(body=file_metadata, media_body=media_body,
                                     fields='id').execute()
            return res.get('id')
        except Exception:
            print(traceback.format_exc())
            sys.exit()

    def fileList(self):
        self._cache = self._files.list(pageSize=20).execute()['files']
        return self._cache

    def getFileId(self, fileName):
        if not self._cache:
            if not self._files:
                self.getFileObject()
            self.fileList()
        for file in self._cache:
            if file['name'] == fileName and (file['mimeType'] == 'application/vnd.google-apps.spreadsheet' or file['mimetype'] == 'text/csv'):
                return file['id']
        return None

if __name__ == '__main__':
    drive = GDrive(
        scopes=[
            'https://www.googleapis.com/auth/drive.file',
            'https://spreadsheets.google.com/feeds'],
        client_secret_file='client_secret.json',
        applicationName='pConnect')
    drive.getCredentials()
    folderName = 'parento'
    folderId = drive.getFolderId(folderName)
    if not folderId:
        folderId = drive.createFolder('parento')
    print (folderId)
    fileId = drive.createFile(
        filePath="/tmp/outDir/pconnect.schools.csv", parentFolderId=folderId)
    files = drive.getFileObject()
    # print (drive.fileList(files))
    """
    file_metadata = {'mimeType': 'application/vnd.google-apps.spreadsheet',
                     'description': 'Just checking',
                     'name': 'pconnect.schools',
                     'parents': [folderId]
                     }
    media_body = MediaFileUpload(
        filename="/tmp/outDir/pconnect.schools.csv",
        mimetype='text/csv',
        resumable=True)
    file = drive.create(body=file_metadata, media_body=media_body,
                        fields='id').execute()
    print (file.get('id'))
    """
