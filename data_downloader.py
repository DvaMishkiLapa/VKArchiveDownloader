import asyncio
import os
from typing import Coroutine, Dict

import aiofiles
import aiohttp

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


async def get_info(url: str, save_path: str, file_name: str, sema: asyncio.BoundedSemaphore) -> Dict[str, str] | None:
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
