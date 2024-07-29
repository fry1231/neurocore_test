import asyncio
import random

from aiohttp import web

from config import MAX_CONCURRENT_UPLOADS, PROCESSING_TIME, HOST, PORT

n_current_uploads = 0


async def process_image(request: web.Request):
    """
    Mocked endpoint to simulate processing an image upload (500 ms)

    :param request: any request
    :return: 200 OK if processed successfully
    :raise 429 Too Many Requests if max concurrent uploads exceeded
    """
    global n_current_uploads  # Will be redis/memcached in production
    # Restrict processing more than MAX_CONCURRENT_UPLOADS at a time
    if n_current_uploads >= MAX_CONCURRENT_UPLOADS:
        return web.Response(status=429,
                            text=f"Too high server load. Please try again later.",
                            headers={"Retry-After": f"{PROCESSING_TIME * MAX_CONCURRENT_UPLOADS}"})

    # Fail randomly to simulate processing errors
    if random.random() < 0.1:
        return web.Response(status=500, text="Failed to process image")

    n_current_uploads += 1
    try:
        # Mock some processing
        await asyncio.sleep(PROCESSING_TIME)
        # =====================
        return web.Response(text=f"Image processed successfully")
    finally:
        n_current_uploads -= 1


app = web.Application()
app.router.add_post('/images/process', process_image)

if __name__ == '__main__':
    web.run_app(app, host=HOST, port=PORT)
