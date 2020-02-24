#!/usr/bin/env python3

"""
Scraper for seafile servers.

This one was written for the rlp.net seafile server, but really should be
general enough to work on others. I just can't be bothered to generalise my
static variables
"""

import os
import requests
import urllib
import asyncio
import json
import time
import argparse
import contextlib

import bs4

BASE_TARGET = "https://seafile.rlp.net/d/{token}"

ZIP_TARGET = "https://seafile.rlp.net/api/v2.1/share-link-zip-task/?share_link_token={token}&path={path}"#&_={time}"
ZIP_PROGRESS = "https://seafile.rlp.net/api/v2.1/query-zip-progress/?token={zip_token}"#&_={time}"
ZIP_FILE = "https://seafile.rlp.net/seafhttp/zip/{zip_token}"
ZIP_CANCEL = "https://seafile.rlp.net/api/v2.1/cancel-zip-task/"

FILE_TARGET = "https://seafile.rlp.net/d/{token}/files/?p={path}&dl=1"

FOLDER_TARGET = "https://seafile.rlp.net/d/{token}/?p={path}&mode=list"

SLEEP_TIME = 10

def attr_check(*keys):
    """Decorator to check that an object has a given attribute set defined by `keys`"""
    def inner_function(method):
        def innerest_function(self, *args, **kwargs):
            for key in keys:
                if getattr(self, key) is None:
                    raise ValueError(f"Calling this method requires the attribute '{key}' to be set")
            return method(self, *args, **kwargs)
        return innerest_function
    return inner_function

def get_files(contents: str) -> list:
    """Get file objects from a seafile html

    Returns a list of dicts that have:

        - the path (very important)
        - link presented on page
        - name presented on page
        - type ('file')
    """
    soup = bs4.BeautifulSoup(contents, 'html.parser')
    return [{'link': i['href'],
             'path': urllib.parse.parse_qs(urllib.parse.urlparse(i['href']).query)['p'][0],
             'name': i.text,
             'type': 'file'} for i in soup.select("tr.file-item a.normal")]

def get_folders(contents: str) -> list:
    """Get folder objects from a seafile html

    Returns a list of dicts that have:

        - the path (very important)
        - link presented on page
        - name presented on page
        - type ('folder')
    """
    soup = bs4.BeautifulSoup(contents, 'html.parser')
    return [{'link': i['href'],
             'path': urllib.parse.parse_qs(urllib.parse.urlparse(i['href']).query)['p'][0],
             'name': i.text,
             'type': 'folder'} for i in soup.select('tr:not(.file-item) a.normal')]


def io_write(filename, content):
    """Function to write the content to file

    Solely exists to allow this to run asyncronously in an executor
    """
    print(f"Writing {filename}")
    filedir = os.path.split(filename)[0]
    if not os.path.exists(filedir):
        os.makedirs(filedir)
    with open(filename, 'wb') as f:
        return f.write(content)

class BaseDownload:
    """Abstract base class for downloads"""
    EXT = ''
    def __init__(self, base=None, path=None, token=None, timeout=60, tries=5, verbose=False, chunking=5, force=False):
        self.verbose = verbose
        self.base = base
        self.path = path
        self.token = token
        self.timeout = timeout
        self.tries = tries
        self.chunking = chunking
        self.force = force
        self.zip_token = None

    def __repr__(self):
        return f"<{type(self).__name__} for '{self.path}' from '{self.token}'>"

    def _print(self, arg):
        if self.verbose:
            print(f"{time.ctime()} {repr(self)} {arg}")

    async def _get(self, uri, okay_fail=[]):
        """Tries self.tries many times to get a thing"""
        tries = -1
        loop = asyncio.get_event_loop()
        while tries < self.tries:
            tries += 1
            if tries > 0:
                await asyncio.sleep(SLEEP_TIME)
            try:
                call_fun = lambda: requests.get(uri, timeout=self.timeout)
                response = await loop.run_in_executor(None, call_fun)
            except requests.exceptions.Timeout:
                self._print(f"Timeout at {uri}")
            except requests.exceptions.ConnectionError:
                self._print(f"Connection error at {uri}")
            except requests.exceptions.MissingSchema:
                self._print(f"URI '{uri}' is malformed.")
                break
            if not response.ok:
                self._print(f"Non-OK status ({response.status_code}) at {uri}")
                if response.status_code in okay_fail:
                    return response
            else:
                return response

    async def get(self):
        raise NotImplementedError

    async def download(self):
        """Download the file (or other get() result) to the path in self.base"""
        target_path = self.base+self.path+self.EXT
        if self.base is not None and (not os.path.exists(target_path) or self.force):
            content = await self.get()
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: io_write(target_path, content))
        elif os.path.exists(target_path):
            self._print(f"File at '{target_path}' already exists")

class Download(BaseDownload):
    """Regular file download

    Just downloads a file from the server
    """
    @attr_check('path', 'token')
    def make_target_uri(self) -> str:
        """Create the target URL of the file"""
        return FILE_TARGET.format(token=self.token, path=self.path)

    async def get(self):
        """File as bytes"""
        uri = self.make_target_uri()
        response = await self._get(uri)
        self._print(f"Got file {self.path}")
        return response.content

class FolderDownload(BaseDownload):
    """Folder listing download"""
    @attr_check('path', 'token')
    def make_target_uri(self) -> str:
        """Create the target URL of the folder"""
        return FOLDER_TARGET.format(token=self.token, path=self.path)

    async def get(self) -> list:
        """Gets the folder listing (dictlist)"""
        uri = self.make_target_uri()
        response = await self._get(uri)
        self._print(f"Got folder listing for resource {self.path}")
        return get_files(response.content) + get_folders(response.content)

class ZipDownload(BaseDownload):
    """Folder as zip download

    This task is perhaps the most complicated one:

        1. Get a token for the folder to zip from
        2. Periodically request status for that token
        3. Download the file
        4. Delete the file (HTTP POST for some reason)

    Steps 1 & 4 are achieved by use of a context manager (self.get_zip_token)
    which is called within get(). Should an 400 error occurr in download, a
    ValueError is raised
    """

    EXT='.zip'

    @attr_check('path', 'token')
    async def initiate_zip(self):
        """Initiate the zip download"""
        uri = ZIP_TARGET.format(path=self.path, token=self.token)
        response = await self._get(uri, okay_fail=[400])
        token_response = json.loads(response.content)
        token_response['requests_status'] = response.status_code
        if response.ok:
            self.zip_token = token_response['zip_token']
        return token_response

    @attr_check('zip_token')
    async def check_zip_status(self):
        """Check status of zip file

        Requires zip_token be set.
        """
        uri = ZIP_PROGRESS.format(zip_token=self.zip_token)
        response = await self._get(uri)
        progress = json.loads(response.content)
        return progress

    @attr_check('zip_token')
    async def get_zip(self):
        """Get the actual zip file.

        Requires zip_token be set.
        """
        uri = ZIP_FILE.format(zip_token=self.zip_token)
        response = await self._get(uri)
        return response.content

    @attr_check('zip_token')
    async def cancel_zip(self):
        """Cancels a zip request, freeing the slot.

        Requires zip_token be set.
        """
        response = requests.post(ZIP_CANCEL, data=f"token={self.zip_token}")
        return response.content

    @contextlib.asynccontextmanager
    async def get_zip_token(self):
        """Zip token context manager.

        Get the zip token for the specified resource and release it (POST to
        delete) on exit.
        """
        resource = await self.initiate_zip()
        try:
            self._print(f"Created context for zip token {resource}")
            yield resource
        finally:
            print(f"Tearing down context for zip token {resource}")
            await self.cancel_zip()
            self._print(f"Tore down context for zip token {resource}")

    async def get(self):
        """Get a zip file

        Requires that `token` and `path` are set.
        Raises a ValueError if it cannot be downloaded for reasons of 400 (malformed request).
        """
        async with self.get_zip_token() as zip_token:
            self.zip_token = zip_token['zip_token']
            if zip_token['requests_status'] == 400:
                raise ValueError(f"The resource at '{self.path}' cannot be zipped because '{zip_token['error_msg']}'")
            else:
                while True:
                    progress = await self.check_zip_status()
                    self._print(f"{self.path} ZIP progress: {progress['zipped']}/{progress['total']}")
                    if progress['zipped'] == progress['total']:
                        break
                    else:
                        await asyncio.sleep(1)
                self._print(f"Getting finished zip...")
                content = await self.get_zip()
                return content

class Scraper(BaseDownload):
    """A seafile scraper

    Scrapes a share token's data
    """
    @attr_check('token')
    async def get(self):
        """Get the token's stuff"""
        targets = [{'type': 'folder', 'path': '/', 'name': 'Root'}]
        while targets:
            current = []
            # Pick new items from targets
            for i in range(self.chunking):
                try:
                    current.append(targets.pop())
                except IndexError:
                    break
            dls = [Download(path=i['path'], token=self.token, verbose=self.verbose, base=self.base, force=self.force)
                   if i['type'] == 'file'
                   else ZipDownload(path=i['path'], token=self.token, verbose=self.verbose, base=self.base, force=self.force)
                   for i in current]
            chunk = [dl.download() for dl in dls]
            runs = await asyncio.gather(*chunk, return_exceptions=True)
            # Check for failures
            fails = [issubclass(type(i), Exception) for i in runs]
            retarget = [current[i] for i, t in enumerate(fails) if t] # These have to be rerun differently
            for fail, failed, obj in zip(runs, fails, current):
                if failed:
                    self._print(f"Failed on {obj['name']} because '{fail}'. Attempting to descend")
            new_targets = [FolderDownload(token=self.token, path=i['path'], verbose=self.verbose, force=self.force).get()
                           for i in retarget
                           if i['type'] == 'folder']
            new_targets = await asyncio.gather(*new_targets, return_exceptions=True)
            targets += sum(new_targets, start=[])


def main():
    """Argument parsing and running of the scraper"""
    parser = argparse.ArgumentParser(description="Scraper for seafile thing")
    parser.add_argument("token", nargs=1, help="Share Token")
    parser.add_argument("--output", "-o", type=str, nargs=1, help="Output directory")
    parser.add_argument("--verbose", "-v", action="store_true", help="Increased verbosity")
    parser.add_argument("--chunk-size", "-c", type=int, default=5, nargs='?',
                        help="Chunk size (max parallel tasks)")
    parser.add_argument("--force", "-f", action='store_true', help="Overwrite files when they exist")
    args = parser.parse_args()

    scraper = Scraper(base=args.output[0],
                      token=args.token[0],
                      verbose=args.verbose,
                      force=args.force,
                      chunking=args.chunk_size)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(scraper.get())

if __name__ == "__main__":
    main()
