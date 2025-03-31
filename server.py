import argparse
import asyncio
import logging
import os

import aiofiles
from aiohttp import client_exceptions, web

logger = logging.getLogger(__name__)


async def create_archive_in_chunks(folder_path, chunk_kb_size=500):
    chunk_size = chunk_kb_size * 1024
    command = ["zip", "-r", "-", ".", folder_path]
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=folder_path,
    )

    try:
        while process.stdout.at_eof():
            chunk = await process.stdout.read(chunk_size)
            logger.debug("Sending archive chunk ...")
            yield chunk

    except asyncio.CancelledError:
        logger.debug("Download was interrupted")
        raise
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def archive(request, delay, photos_dir):
    archive_hash = request.match_info.get["archive_hash"]
    folder_path = f"{photos_dir}/{archive_hash}/"
    if not os.path.exists(folder_path):
        logger.error(f"Directory not found: {folder_path}")
        raise web.HTTPNotFound(text="Архив не существует или был удален")

    response = web.StreamResponse()
    response.headers["Content-Type"] = "application/zip"
    response.headers["Content-Disposition"] = 'attachment; filename="photos.zip"'
    await response.prepare(request)

    try:
        async for chunk in create_archive_in_chunks(folder_path):
            await response.write(chunk)
            await asyncio.sleep(delay)
    except IndexError as e:
        logger.error(f"Request Error: {e}")
        raise web.HTTPBadRequest(text=str(e))
    except asyncio.CancelledError as e:
        logger.warning(f"The request was aborted: {e}")
        raise web.HTTPInternalServerError(text="Запрос был прерван")
    except SystemExit as e:
        logger.critical(f"Program termination is requested: {e}")
        raise web.HTTPInternalServerError(text="Внутренняя ошибка сервера")
    except (
        client_exceptions.ClientConnectionResetError,
        client_exceptions.ClientError,
    ) as e:
        logger.error(f"The client terminated the connection: {e}")
        raise web.HTTPClientError(text="Клиент разорвал соединение")
    except Exception as e:
        logger.error(f"Unknown Error: {e}")
        raise web.HTTPInternalServerError(text="Внутренняя ошибка сервера")

    return response


async def handle_index_page(request):
    async with aiofiles.open("index.html", mode="r") as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type="text/html")


def parse_args():
    parser = argparse.ArgumentParser(description="Сервер для архивации фотографий")
    parser.add_argument(
        "--log",
        action="store_true",
        help="Включить логирование (по умолчанию: выключено)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Задержка между отправкой фрагментов в секундах (по умолчанию: 0.1)",
    )
    parser.add_argument(
        "--photos_dir",
        type=str,
        default="./test_photos",
        help="Путь к каталогу с фотографиями (по умолчанию: ./test_photos)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.log:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
    app = web.Application()
    app.add_routes(
        [
            web.get("/", handle_index_page),
            web.get(
                "/archive/{archive_hash}/",
                lambda request: archive(request, args.delay, args.photos_dir),
            ),
        ]
    )
    web.run_app(app)
