from __future__ import print_function
import os.path
import pickle
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta
import threading
import logging
from typing import Any, Dict, Optional
from ..base import BaseTool

class GoogleLink(BaseTool):
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    
    def __init__(self):
        super().__init__()
        self.service = self.authenticate_google_drive()

    def authenticate_google_drive(self):
        creds = None
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', self.SCOPES)
                creds = flow.run_local_server(port=0)
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        return build('drive', 'v3', credentials=creds)

    def upload_file(self, file_path):
        file_metadata = {'name': os.path.basename(file_path)}
        media = MediaFileUpload(file_path, mimetype='application/octet-stream')
        file = self.service.files().create(body=file_metadata,
                                           media_body=media,
                                           fields='id').execute()
        return file.get('id')

    def generate_shareable_link(self, file_path, valid_time):
        file_id = self.upload_file(file_path)
        request_body = {
            'role': 'reader',
            'type': 'anyone'
        }
        self.service.permissions().create(
            fileId=file_id,
            body=request_body
        ).execute()

        response_link = self.service.files().get(
            fileId=file_id,
            fields='webViewLink'
        ).execute()
        
        expiration_time = datetime.now() + timedelta(days=valid_time[0],weeks=valid_time[1],hours=valid_time[2],minutes=valid_time[3],seconds=valid_time[4]) 
        self.set_expiration(file_id, expiration_time)

        return response_link.get('webViewLink')

    def revoke_shareable_link(self, file_id):
        permissions = self.service.permissions().list(fileId=file_id).execute()
        for permission in permissions.get('permissions', []):
            if permission['role'] == 'reader' and permission['type'] == 'anyone':
                self.service.permissions().delete(fileId=file_id, permissionId=permission['id']).execute()

    def set_expiration(self, file_id, expiration_time):
        delay = (expiration_time - datetime.now()).total_seconds()
        threading.Timer(delay, self.revoke_shareable_link, [file_id]).start()