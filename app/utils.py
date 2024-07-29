import os
import aiofiles


async def file_sender(file_path: str | os.PathLike,
                      chunk_size: int = 64*1024):
    """
    Read file from disk asynchronously and yield chunks of data
    Allows to send large files without loading the entire file into memory
    """
    if not os.path.isfile(file_path):
        print(f"File {file_path} not found")
        raise FileNotFoundError(f"File {file_path} not found")

    async with aiofiles.open(file_path, mode='rb') as f:
        chunk = await f.read(chunk_size)
        while chunk:
            yield chunk
            chunk = await f.read(chunk_size)
