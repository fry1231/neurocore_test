import os
import queue
import threading
from time import sleep

from requests import Response, Session
from tenacity import retry, stop_after_attempt, RetryError

from clients._base import AbstractUploadClient
from config import HOST, PORT, BASE_PATH


class ThreadUploadClient(AbstractUploadClient):
    """
    Thread client for uploading images to a server
    Uses a semaphore to limit the number of concurrent uploads
    """

    def __init__(self,
                 url: str,
                 max_concurrent_uploads: int,
                 session: Session | None = None,
                 client_timeout: int = 5):
        super().__init__(
            url=url,
            session=session,
            max_concurrent_uploads=max_concurrent_uploads,
            client_timeout=client_timeout
        )
        self.queue = queue.Queue()  # Queue to hold image paths
        self.results: list[tuple[str, Response, str]] = []
        self.results_lock = threading.Lock()  # Lock to ensure thread-safe access to results
        self.session = Session() if self.session is None else self.session
        self.threads = []  # List to keep track of worker threads

    def _reset_results(self):
        """Should be called before each upload_images call to reset the results list"""
        self.results = []

    @retry(stop=stop_after_attempt(3))
    def _upload_image(self,
                      image_path: str | os.PathLike) -> Response:
        """
        Uploads an image to the server
        Retries up to 3 times if upload fails
        """
        with open(image_path, 'rb') as f:
            response: Response = self.session.post(self.url, files={'file': f}, timeout=self.client_timeout)
        print(status := response.status_code)
        if status == 429:
            # Wait if server is overloaded, then retry
            sleep_time = float(response.headers.get('Retry-After', '1'))
            print(message := f"Server is overloaded. Waiting for {sleep_time} seconds.")
            sleep(sleep_time)
            raise ConnectionError(message)
        elif status >= 500:
            raise ConnectionRefusedError(f"Failed to upload image {image_path}."
                                         f" Server returned status {status}")
        return response

    def _limited_upload_image(self) -> None:
        """
        Worker function to process images from the queue

        :raises RetryError: if upload fails after 3 attempts
        """
        while True:
            image_path = self.queue.get()
            if image_path is None:
                self.queue.task_done()
                break
            try:
                response = self._upload_image(image_path)
                with self.results_lock:
                    self.results.append((image_path, response, response.text))
            except RetryError as e:
                print(f"Failed to upload image {image_path} after several attempts.")
            finally:
                self.queue.task_done()

    def upload_images(self,
                      image_paths: list[str | os.PathLike]) -> list[Response]:
        """
        Upload images concurrently, with a limit on the number of concurrent uploads
        Starts a `max_concurrent_uploads` number of worker threads to process the images
        Uses queue to hold image paths and `self.results` list with locking to store the responses

        :param image_paths: list of image paths to upload
        :return: list of request.Response objects, one for each image upload in the order of the input list
        """
        # Reset results list before each upload
        self._reset_results()

        # Start `max_concurrent_uploads` worker threads
        for _ in range(self.max_concurrent_uploads):
            # Create a thread for each worker function
            thread = threading.Thread(target=self._limited_upload_image)
            thread.start()
            self.threads.append(thread)

        # Add all image paths to the queue
        for image_path in image_paths:
            self.queue.put(image_path)

        # Wait for all tasks to be processed
        self.queue.join()

        # Stop all worker threads
        for _ in range(self.max_concurrent_uploads):
            self.queue.put(None)  # Add None to the queue to signal threads to stop
        for thread in self.threads:
            thread.join()  # Wait for all threads to finish

        # sort the results by the order of the image paths
        self.results.sort(key=lambda x: image_paths.index(x[0]))
        # leave only the response objects
        self.results = [r[1] for r in self.results]
        return self.results  # Return the results of the uploads


if __name__ == '__main__':
    url: str = f'http://{HOST}:{PORT}/images/process'
    image_paths = [os.path.join(BASE_PATH, 'image.jpg') for i in range(10)]
    client = ThreadUploadClient(url, max_concurrent_uploads=8)
    res: list[Response] = client.upload_images(image_paths)
    print(len(res))
    print('\n'.join([str((i, r.text)) for i, r in enumerate(res)]))
