import os
from abc import ABC, abstractmethod
from typing import TypeVar

import aiohttp
import requests

AnySession = TypeVar('AnySession', aiohttp.ClientSession, requests.Session)
AnyResponse = TypeVar('AnyResponse', aiohttp.ClientResponse, requests.Response)


class AbstractUploadClient(ABC):
    """
    Abstract base class for upload clients
    Implements the upload_images method to upload multiple images concurrently
    Uses sessions to manage connections and semaphores to limit the number of concurrent uploads
    """

    def __init__(self,
                 url: str,
                 max_concurrent_uploads: int,
                 session: AnySession | None = None,
                 client_timeout: int = 5):
        self.url = url
        self.client_timeout = client_timeout
        self.max_concurrent_uploads = max_concurrent_uploads
        self.session = session

    @abstractmethod
    async def _upload_image(self, image_path: str | os.PathLike) -> AnyResponse:
        """
        Uploads an image to the server
        """
        pass

    @abstractmethod
    async def _limited_upload_image(self, *args, **kwargs) -> AnyResponse | None:
        """
        Limit the number of concurrent uploads
        """
        pass

    @abstractmethod
    async def upload_images(self, image_paths: list[str | os.PathLike]) -> list[AnyResponse]:
        """
        Upload images concurrently, with a limit on the number of concurrent uploads

        :param image_paths: list of image paths to upload
        :return: list of responses from the server
        """
        pass
