import asyncio
from os.path import join
import traceback
from typing import Coroutine, Dict

import aiofiles
import aiohttp
from bs4 import BeautifulSoup

import tools
from logger import create_logger

logger = create_logger('logs/vk_parser.log', 'data_downloader', 'DEBUG')


def get_response_info(content_type: str) -> Dict[str, str]:
    '''
    Возвращает информацию из заголовка запроса `content-type`
    `content_type`: заголовок запроса

    Максимальная информация, которая может быть возвращена:
    ```
    {
        'encoding': 'Кодировка контента, если он текстовый, иначе ключ будет отсутствовать',
        'data_type': 'Тип контента, например, text или image',
        'extension': 'Конкретное расширение для контента, например, html или gif',
        'full_type_info': 'Комбинация data_type и extension через /'
    }
    ```
    '''
    result = {}
    find_res = content_type.find('charset=')
    if find_res != -1:
        result.update({'encoding': content_type[find_res + len('charset='):]})
    find_res = content_type.find(';')
    if find_res != -1:
        full_type_info = content_type[:find_res]
        data_type, extension = full_type_info.split('/')
    else:
        full_type_info = content_type
        data_type, extension = full_type_info.split('/')
    result.update({
        'data_type': data_type,
        'extension': extension,
        'full_type_info': full_type_info
    })
    return result


async def find_link_by_url(session: aiohttp.ClientSession, url: str) -> str:
    '''
    Находит ссылку на документ VK
    `session`: сессия
    `url`: ссылка на документ VK, где нужно найти ссылку
    '''
    async with session.get(url, timeout=5) as response:
        assert response.status == 200, f'Response status: {response.status}'
        soup = BeautifulSoup(await response.text(), 'html.parser')
        assert 'Ошибка' not in soup.find('title').text, 'Ошибка доступа к документу'
        return soup.find('img').get('src')


async def downloader(response: aiohttp.ClientResponse, path: str, name: str) -> Coroutine:
    '''
    Скачивает файл из `response`:
    `response`: ответ на запрос
    `path`: путь, куда будет сохранен файл
    `name`: имя сохраняемого файла
    '''
    async with aiofiles.open(join(path, name), 'wb') as f:
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
    `sema`: семафор для асинхронного скачивания
    '''
    try:
        async with sema, aiohttp.ClientSession(headers={'Accept-Language': 'ru'}) as session:
            async with session.get(url, timeout=15) as response:
                assert response.status == 200, f'Response status: {response.status}'
                if any(t in response.headers['content-type'] for t in ('image', 'audio')):
                    response_info = get_response_info(response.headers['content-type'])
                    download_path = join(save_path, response_info['full_type_info'])
                    tools.create_folder(download_path)
                    await asyncio.create_task(
                        downloader(
                            response=response,
                            path=download_path,
                            name=f'{file_name}.{response_info["extension"]}'
                        )
                    )
                    return {'url': response.url, 'file_info': response_info['full_type_info']}
                target_content_type = response.headers['content-type']
            if 'text/html' in target_content_type and 'vk.com/doc' in url:
                link = await asyncio.create_task(find_link_by_url(session, response.url))
                async with session.get(link, timeout=15) as response:
                    response_info = get_response_info(response.headers['content-type'])
                    download_path = join(save_path, response_info['full_type_info'])
                    tools.create_folder(download_path)
                    await asyncio.create_task(
                        downloader(
                            response=response,
                            path=download_path,
                            name=f'{file_name}.{response_info["extension"]}'
                        )
                    )
                    return {'url': response.url, 'file_info': response_info['full_type_info']}
            return {'url': response.url, 'file_info': 'not_parse'}
    except Exception as e:
        logger.error(f'Ошибка 🔗 {url}: {e}')
        logger.debug(traceback.format_exc())
        return {'url': url, 'file_info': 'error'}
