import dropbox
import logging
import requests
from requests.auth import HTTPBasicAuth
import os
import time
import fnmatch

# Configure logging
#logging.basicConfig(filename='dropbox_client.log', level=self.logger.info,
#                    format='%(asctime)s - %(levelname)s - %(message)s')


class DropboxClient:
    def __init__(self, refresh_token=None, client_id=None, client_secret=None, max_retries=3, retry_delay=2):
        """
        Initialize the DropboxClient. Environment variables are used by default,
        but they can be overridden by providing values directly.

        :param refresh_token: Optional. The Dropbox refresh token.
        :param client_id: Optional. The Dropbox client ID.
        :param client_secret: Optional. The Dropbox client secret.
        :param max_retries: Maximum number of retries for operations.
        :param retry_delay: Initial delay in seconds between retries, with exponential backoff.
        """
        self.logger=logging.getLogger('DropboxClient')
        self.refresh_token = refresh_token or os.getenv('DROPBOX_REFRESH_TOKEN')
        self.client_id = client_id or os.getenv('DROPBOX_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('DROPBOX_CLIENT_SECRET')
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        if not all([self.refresh_token, self.client_id, self.client_secret]):
            raise ValueError("Missing required environment variables or parameters for Dropbox credentials.")

        self.access_token = self._get_access_token()
        self.dbx = dropbox.Dropbox(self.access_token)
        
        self.logger.info("DropboxClient initialized.")

    def _check_access_token(self):
        """Ensure the access token is valid or refresh it if expired."""
        if not self.access_token or not self._is_access_token_valid():
            self.logger.info("Access token invalid or expired. Refreshing token...")
            self._refresh_access_token()

        if not self._is_access_token_valid():
            self.logger.error("Access token could not be refreshed. Please check credentials.")
            raise ValueError("Access token invalid, and refresh failed.")

    def _is_access_token_valid(self):
        """Validate the current access token by making a simple API call."""
        try:
            self.dbx.users_get_current_account()
            return True
        except dropbox.exceptions.AuthError:
            self.logger.info("Access token is invalid or expired.")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error while validating token: {e}")
            return False

    def _get_access_token(self):
        """Obtain a new Dropbox access token using the refresh token."""
        try:
            response = requests.post(
                'https://api.dropbox.com/oauth2/token',
                data={'grant_type': 'refresh_token', 'refresh_token': self.refresh_token},
                auth=HTTPBasicAuth(self.client_id, self.client_secret)
            )
            response.raise_for_status()
            token_data = response.json()
            return token_data.get('access_token')
        except (requests.RequestException, KeyError) as e:
            self.logger.error(f"Error obtaining access token: {e}")
            raise

    def _refresh_access_token(self):
        """Refresh the Dropbox access token and update the Dropbox client."""
        self.access_token = self._get_access_token()
        self.dbx = dropbox.Dropbox(self.access_token)
        self.logger.info("Access token refreshed successfully.")

    def _retry_operation(self, operation, *args, **kwargs):
        """
        Retry wrapper for operations that might fail.
        Retries the given operation up to 'max_retries' times with exponential backoff.

        :param operation: The operation (method) to retry.
        :param args: Positional arguments to pass to the operation.
        :param kwargs: Keyword arguments to pass to the operation.
        """
        attempt = 0
        while attempt < self.max_retries:
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                attempt += 1
                wait_time = self.retry_delay * (2 ** (attempt - 1))
                self.logger.error(f"Attempt {attempt} failed: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        raise Exception(f"Operation failed after {self.max_retries} attempts.")

    def upload_file(self, local_file_path, dropbox_file_path):
        """
        Upload a file to Dropbox with retries.

        :param local_file_path: Path to the local file to upload.
        :param dropbox_file_path: Path in Dropbox where the file will be uploaded.
        """
        self._check_access_token()

        def _upload():
            with open(local_file_path, 'rb') as file:
                self.dbx.files_upload(file.read(), dropbox_file_path)
            self.logger.info(f"File '{local_file_path}' uploaded to '{dropbox_file_path}'.")
            print(f"File '{local_file_path}' uploaded to '{dropbox_file_path}'.")
        try:
            self._retry_operation(_upload)
        except FileNotFoundError:
            self.logger.error(f"File '{local_file_path}' not found.")
            print(f"File '{local_file_path}' not found.")
        except dropbox.exceptions.ApiError as e:
            self.logger.error(f"Dropbox API error during upload: {e}")
            print(f"Dropbox API error during upload: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during file upload: {e}")
            print(f"Unexpected error during file upload: {e}")

    def download_file(self, dropbox_file_path, local_file_path=None):
        """
        Download a file from Dropbox with retries.

        :param dropbox_file_path: Path in Dropbox of the file to download.
        :param local_file_path: Path where the file will be saved locally.
        """
        self._check_access_token()
        # If local_file_path is not provided, construct it using the dropbox_file_path's file name
        if local_file_path is None:
            local_file_name = os.path.basename(dropbox_file_path)
            local_file_path = os.path.join(os.getcwd(), local_file_name)
            self.logger.info(f"No local_file_path provided. Using default path: {local_file_path}")
        def _download():
            metadata, res = self.dbx.files_download(dropbox_file_path)
            with open(local_file_path, 'wb') as file:
                file.write(res.content)
            self.logger.info(f"File '{dropbox_file_path}' downloaded to '{local_file_path}'.")

        try:
            self._retry_operation(_download)
        except dropbox.exceptions.ApiError as e:
            self.logger.error(f"Dropbox API error during download: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during file download: {e}")

    def upload_folder(self, local_folder_path, dropbox_folder_path, filename_pattern=None):
        """
        Upload a folder and its contents to Dropbox with retries, optionally filtering files by pattern.

        :param local_folder_path: Path to the local folder to upload.
        :param dropbox_folder_path: Path in Dropbox where the folder and files will be uploaded.
        :param filename_pattern: Optional filename pattern to filter files for uploading (e.g., '*.txt').
        """
        self._check_access_token()

        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                if filename_pattern is None or fnmatch(file, filename_pattern):
                    local_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_file_path, local_folder_path)
                    dropbox_file_path = f"{dropbox_folder_path}/{relative_path}".replace("\\", "/")
                    self.upload_file(local_file_path, dropbox_file_path)

    def download_folder(self, dropbox_folder_path, local_folder_path, filename_pattern=None):
        """
        Download a folder and its contents from Dropbox with retries, optionally filtering files by pattern.

        :param dropbox_folder_path: Path in Dropbox of the folder to download.
        :param local_folder_path: Path where the folder and files will be saved locally.
        :param filename_pattern: Optional filename pattern to filter files for downloading (e.g., '*.txt').
        """
        self._check_access_token()

        try:
            result = self.dbx.files_list_folder(dropbox_folder_path, recursive=True)
            while True:
                for entry in result.entries:
                    if isinstance(entry, dropbox.files.FileMetadata):
                        if filename_pattern is None or fnmatch(entry.name, filename_pattern):
                            local_file_path = os.path.join(local_folder_path, entry.path_lower[len(dropbox_folder_path):].lstrip('/'))
                            local_dir = os.path.dirname(local_file_path)
                            if not os.path.exists(local_dir):
                                os.makedirs(local_dir)
                            self.download_file(entry.path_lower, local_file_path)

                if not result.has_more:
                    break
                result = self.dbx.files_list_folder_continue(result.cursor)
            self.logger.info(f"Folder '{dropbox_folder_path}' downloaded to '{local_folder_path}'.")

        except dropbox.exceptions.ApiError as e:
            self.logger.error(f"Dropbox API error during folder download: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during folder download: {e}")

    def list_files(self, folder_path, filename_pattern=None):
        """
        List files in a Dropbox folder with their last updated datetime.

        :param folder_path: Path in Dropbox of the folder to list files from.
        :param filename_pattern: Optional pattern to filter filenames (e.g., '*.txt').
        :return: List of tuples (filename, last updated datetime).
        """
        self._check_access_token()

        files_list = []
        try:
            result = self.dbx.files_list_folder(folder_path)
            while True:
                for entry in result.entries:
                    if isinstance(entry, dropbox.files.FileMetadata):
                        if filename_pattern is None or fnmatch(entry.name, filename_pattern):
                            files_list.append((entry.name, entry.server_modified))

                if not result.has_more:
                    break
                result = self.dbx.files_list_folder_continue(result.cursor)
            return files_list
        except dropbox.exceptions.ApiError as e:
            self.logger.error(f"Dropbox API error during listing files: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during file listing: {e}")
            raise

    def remove_file(self, dropbox_path):
        """
        Remove a file from Dropbox with retries.

        :param dropbox_path: The path of the file in Dropbox to be removed.
        """
        self._check_access_token()

        def _remove():
            self.dbx.files_delete_v2(dropbox_path)
            self.logger.info(f"File removed from Dropbox: {dropbox_path}")

        try:
            self._retry_operation(_remove)
        except dropbox.exceptions.ApiError as e:
            self.logger.error(f"Dropbox API error during file removal: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during file removal: {e}")


    def rename_file(self, dropbox_path, new_name):
        """
        Rename a file in Dropbox with retries.

        :param dropbox_path: The current path of the file.
        :param new_name: The new name for the file (within the same folder).
        """
        self._check_access_token()

        def _rename():
            new_path = os.path.join(os.path.dirname(dropbox_path), new_name)
            self.dbx.files_move_v2(dropbox_path, new_path)
            self.logger.info(f"File renamed in Dropbox: {dropbox_path} to {new_path}")

        try:
            self._retry_operation(_rename)
        except dropbox.exceptions.ApiError as e:
            self.logger.error(f"Dropbox API error during file rename: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during file rename: {e}")


    def get_most_recent_file(self, folder_path):
        """
        Get the full path of the most recent file in a Dropbox folder with retries.

        :param folder_path: The path of the folder in Dropbox.
        :return: The path of the most recently modified file in the folder, or None if no files are found.
        """
        self._check_access_token()

        def _get_recent():
            files = self.dbx.files_list_folder(folder_path).entries
            files = [f for f in files if isinstance(f, dropbox.files.FileMetadata)]
            if not files:
                return None
            most_recent_file = max(files, key=lambda f: f.server_modified)
            self.logger.info(f"Most recent file in Dropbox: {most_recent_file.path_lower}")
            return most_recent_file.path_lower

        try:
            return self._retry_operation(_get_recent)
        except dropbox.exceptions.ApiError as e:
            self.logger.error(f"Dropbox API error during fetching the most recent file: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during fetching the most recent file: {e}")
            return None
    def file_exists(self, dropbox_path):
        """
        Check if a file exists in Dropbox with retries.

        :param dropbox_path: The path of the file in Dropbox.
        :return: True if the file exists, False otherwise.
        """
        self._check_access_token()

        def _check():
            try:
                self.dbx.files_get_metadata(dropbox_path)
                self.logger.info(f"File exists in Dropbox: {dropbox_path}")
                return True
            except dropbox.exceptions.ApiError as e:
                if isinstance(e.error, dropbox.files.GetMetadataError):
                    self.logger.info(f"File does not exist in Dropbox: {dropbox_path}")
                    return False
                else:
                    self.logger.error(f"Dropbox API error during file existence check: {e}")
                    raise e

        try:
            return self._retry_operation(_check)
        except Exception as e:
            self.logger.error(f"Unexpected error during file existence check: {e}")
            return False
