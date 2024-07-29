import asyncio
import os

import aiohttp
from tenacity import retry, stop_after_attempt, RetryError

from clients._base import AbstractUploadClient
from config import HOST, PORT, BASE_PATH
from utils import file_sender


class AsyncUploadClient(AbstractUploadClient):
    """
    Asynchronous client for uploading images to a server
    Uses a semaphore to limit the number of concurrent uploads
    """

    def __init__(self,
                 url: str,
                 max_concurrent_uploads: int,
                 session: aiohttp.ClientSession | None = None,
                 client_timeout: int = 5):
        super().__init__(
            url=url,
            max_concurrent_uploads=max_concurrent_uploads,
            session=session,
            client_timeout=client_timeout
        )
        self.semaphore = asyncio.Semaphore(max_concurrent_uploads)
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(self.client_timeout))

    @retry(stop=stop_after_attempt(3))
    async def _upload_image(self,
                            image_path: str | os.PathLike) -> aiohttp.ClientResponse:
        """
        Uploads an image to the server
        Retries up to 3 times if upload fails
        """
        response = await self.session.post(self.url, data=file_sender(image_path))
        print(response.status)
        if response.status == 429:
            sleep_time = float(response.headers.get('Retry-After', '1'))
            print(message := f"Server is overloaded. Waiting for {sleep_time} seconds.")
            await asyncio.sleep(sleep_time)
            raise ConnectionError(message)
        elif response.status >= 500:
            raise ConnectionRefusedError(f"Failed to upload image {image_path}. "
                                         f"Server returned status {response.status}")
        return response

    async def _limited_upload_image(self, image_path: str | os.PathLike) -> aiohttp.ClientResponse:
        """
        Uses a semaphore to limit the number of concurrent uploads
        If upload fails, retries up to 3 times

        :param image_path:
        :return: ClientResponse object with the server response
        :raises RetryError: if upload fails after 3 attempts
        """
        async with self.semaphore:
            try:
                return await self._upload_image(image_path)
            except RetryError:
                print(f"Failed to upload image {image_path} after several attempts.")
                raise

    async def upload_images(self,
                            image_paths: list[str | os.PathLike]) -> list[aiohttp.ClientResponse]:
        """
        Upload images concurrently, with a limit on the number of concurrent uploads

        :param image_paths: list of image paths to upload
        :return: list of aiohttp.ClientResponse objects, one for each image upload in the order of the input list
        """
        async with self.session:
            async with asyncio.TaskGroup() as tg:
                tasks = [tg.create_task(self._limited_upload_image(image_path)) for image_path in image_paths]
                results = await asyncio.gather(*tasks)
                return results


if __name__ == '__main__':
    url: str = f'http://{HOST}:{PORT}/images/process'
    image_paths = [os.path.join(BASE_PATH, 'image.jpg') for i in range(10)]
    client = AsyncUploadClient(url, max_concurrent_uploads=5)
    res: list[aiohttp.ClientResponse] = asyncio.run(client.upload_images(image_paths))
    print(len(res))
    print('\n'.join([str((i, asyncio.run(r.text()))) for i, r in enumerate(res)]))
