import asyncio
import os
from concurrent.futures import ProcessPoolExecutor
from typing import Coroutine, Dict, List

import aiofiles
import aiohttp
import requests

import tools
from logger import create_logger

logger = create_logger('logs/vk_parser.log', 'data_downloader', 'DEBUG')


async def downloader(response: aiohttp.ClientResponse, path: str, name: str) -> Coroutine:
    '''
    Скачивает файл из `response`:
    `response`: ответ на запрос
    `path`: путь, куда будет сохранен файл
    `name`: имя сохраняемого файла
    '''
    async with aiofiles.open(os.path.join(path, name), 'wb') as f:
        async for data in response.content.iter_any():
            await f.write(data)


async def get_info(url: str, save_path: str, file_name: str, sema: asyncio.BoundedSemaphore) -> Dict[str, str]:
    '''
    Скачивает файл из `response`, возвращая о нем информацию:
    ```
    {
        'url': URL скаченного файла,
        'file_info': информация о типе файла
    }
    ```

    `url`: ссылка на файл или ресурс
    `save_path`: путь до папки, куда будет сохранен файл
    `file_name`: имя файла
    `sema`: семафор для асинхронного скачивания
    '''
    try:
        async with sema, aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                assert response.status == 200, f'Response status: {response.status}'
                file_info = response.headers['content-type'].split(';')[0]
                file_type = file_info.split('/')[-1]
                if any(t in response.headers['content-type'] for t in ('image', 'audio')):
                    download_path = os.path.join(save_path, file_type)
                    tools.create_folder(os.path.join(download_path))
                    await asyncio.create_task(
                        downloader(
                            response=response,
                            path=download_path,
                            name=f'{file_name}.{file_type}'
                        )
                    )
                return {'url': response.url, 'file_info': file_info}
    except Exception as e:
        logger.error(f'Ошибка 🔗 {url}: {e}')
        return {'url': url, 'file_info': 'error'}


def multi_get_executor(urls: List[str], save_path: str, core_count: int) -> List[dict] | None:
    data = [
        {
            'url': url,
            'path': save_path,
            'name': str(name)
        } for name, url in enumerate(urls)
    ]
    with ProcessPoolExecutor(core_count) as executor:
        return list(executor.map(sync_get_info, data))


def sync_downloader(response: requests.Response, path: str, name: str) -> None:
    '''
    Скачивает файл из `response` в синхронном режиме:
    `response`: ответ на запрос
    `path`: путь, куда будет сохранен файл
    `name`: имя сохраняемого файла
    '''
    with open(os.path.join(path, name), 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def sync_get_info(data: Dict[str, str]) -> Dict[str, str]:
    try:
        with requests.Session() as session:
            with session.get(data['url'], timeout=5) as response:
                assert response.status_code == 200, f'Response status: {response.status_code}'
                file_info = response.headers['content-type'].split(';')[0]
                file_type = file_info.split('/')[-1]
                if any(t in response.headers['content-type'] for t in ('image', 'audio')):
                    download_path = os.path.join(data['path'], file_type)
                    tools.create_folder(os.path.join(download_path))
                    sync_downloader(
                        response=response,
                        path=download_path,
                        name=f'{data["name"]}.{file_type}'
                    )
                    return {'url': response.url, 'file_info': file_info}
    except Exception as e:
        logger.error(f'Ошибка 🔗 {data["url"]}: {e}')
        return {'url': data["url"], 'file_info': 'error'}
