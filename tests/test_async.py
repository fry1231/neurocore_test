from time import time
from unittest.mock import patch

import pytest

from config import MAX_CONCURRENT_UPLOADS, PROCESSING_TIME


class AsyncGenMock:
    """
    Async generator, mocking the utils.file_sender function to simulate streaming file content
    """

    def __init__(self, *args, **kwargs):
        self.iter = iter(args)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration


@pytest.fixture
def async_mock_open_images():
    with patch('utils.file_sender', return_value=AsyncGenMock(b'file content')) as mock_file_sender:
        yield mock_file_sender


@pytest.fixture
def init_async():
    """
    Initialize the server and clients for testing
    """
    from server import app
    from clients.async_client import AsyncUploadClient
    from clients.thread_client import ThreadUploadClient

    return app, AsyncUploadClient, ThreadUploadClient


@pytest.mark.asyncio
async def test_upload_async(aiohttp_client, async_mock_open_images, init_async):
    app, AsyncUploadClient, _ = init_async
    url = f'/images/process'
    image_paths = [f'/path/to/image/{i}.jpg' for i in range(3 * MAX_CONCURRENT_UPLOADS)]
    client = await aiohttp_client(app)

    t0 = time()
    upload_async = AsyncUploadClient(
        url=url,
        max_concurrent_uploads=MAX_CONCURRENT_UPLOADS,
        session=client
    )
    results = await upload_async.upload_images(image_paths=image_paths)
    t1 = time()

    # Ensure requests are async
    assert t1 - t0 < PROCESSING_TIME * len(image_paths)
    # All requests should be processed
    assert len(results) == len(image_paths)
    assert all(result.status == 200 for result in results)

    # Test that the server cannot handle more than MAX_CONCURRENT_UPLOADS
    t1 = time()
    client = await aiohttp_client(app)
    upload_async = AsyncUploadClient(
        url=url,
        max_concurrent_uploads=2 * MAX_CONCURRENT_UPLOADS,
        session=client
    )
    results = await upload_async.upload_images(image_paths=image_paths)
    t2 = time()

    assert len(results) == len(image_paths)

    # Every failed with 5xx and 429 request should be repeated
    assert all(result.status < 429 for result in results)

    time_first_batch = t1 - t0
    time_second_batch = t2 - t1

    # The second batch should take longer because of the 429 retry-after waiting
    assert time_second_batch > time_first_batch
