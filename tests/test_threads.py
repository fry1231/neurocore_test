import os
import sys
from subprocess import Popen, PIPE
from time import sleep, time
from unittest import mock as mocker
from unittest.mock import patch

import pytest

from config import MAX_CONCURRENT_UPLOADS, HOST, PORT, PROCESSING_TIME


@pytest.fixture
def mock_open_images():
    mocked_data = mocker.mock_open(read_data="file content")
    with patch('builtins.open', mocked_data) as mock_open:
        yield


@pytest.fixture
def init_async():
    """
    Initialize the server and clients for testing
    """
    from server import app
    from clients.async_client import AsyncUploadClient
    from clients.thread_client import ThreadUploadClient

    return app, AsyncUploadClient, ThreadUploadClient


@pytest.fixture
def init_sync(init_async):
    python_executable = sys.executable
    app, _, ThreadUploadClient = init_async
    url = f'http://{HOST}:{PORT}/images/process'

    # Start the server in a separate process
    process = Popen([python_executable, 'app/server.py'], stdout=PIPE, stderr=PIPE, env=os.environ)

    sleep(1)  # Give the server some time to start

    yield ThreadUploadClient, url

    process.terminate()
    process.wait()


@pytest.mark.asyncio
async def test_upload_threaded(mock_open_images, init_sync):
    ThreadUploadClient, url = init_sync
    image_paths = [f'/path/to/image/{i}.jpg' for i in range(3 * MAX_CONCURRENT_UPLOADS)]
    t0 = time()

    # Normal case: thread_limit == MAX_CONCURRENT_UPLOADS
    upload_threaded = ThreadUploadClient(url=url,
                                         max_concurrent_uploads=MAX_CONCURRENT_UPLOADS)
    results = upload_threaded.upload_images(image_paths=image_paths)
    t1 = time()

    # Ensure threading works
    assert t1 - t0 < PROCESSING_TIME * len(image_paths)

    # All requests should be processed
    assert len(results) == len(image_paths)
    assert all(result.status_code == 200 for result in results)

    # Test that the server cannot handle more than MAX_CONCURRENT_UPLOADS
    upload_threaded = ThreadUploadClient(url=url,
                                         max_concurrent_uploads=2 * MAX_CONCURRENT_UPLOADS)
    results = upload_threaded.upload_images(image_paths=image_paths)
    t2 = time()

    # All requests should be processed
    assert len(results) == len(image_paths)
    assert all(result.status_code < 429 for result in results)

    # The second batch should take longer because of the 429 retry-after waiting
    time_first_batch = t1 - t0
    time_second_batch = t2 - t1
    assert time_second_batch > time_first_batch
