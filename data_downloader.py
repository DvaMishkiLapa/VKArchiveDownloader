import asyncio
from configparser import ConfigParser
from os.path import join
from traceback import format_exc
from typing import Coroutine, Dict

import aiofiles
import aiohttp
from bs4 import BeautifulSoup

import tools
from logger import create_logger

config = ConfigParser()
config_read = config.read('config.ini', encoding='utf8')
if config_read is None:
    logger = create_logger('logs/vk_parser.log', 'data_downloader', 'DEBUG')
else:
    log_level = config['main_parameters'].get('log_level', 'DEBUG')
    logger = create_logger('logs/vk_parser.log', 'data_downloader', log_level)


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


def get_file_name_by_link(link: str) -> str | None:
    try:
        if '?extra=' in link:
            return link.split('/')[-1].split('?extra=')[0]
        elif '?size=' in link:
            return link.split('/')[-1].split('?size=')[0]
        else:
            return link.split('/')[-1]
    except Exception:
        return None


def check_vk_title_error(soup: BeautifulSoup) -> bool:
    '''
    Проверяет наличие ошибки доступа к содержимому VK
    `soup`: экземпляр `BeautifulSoup` для работы с `HTML` контентом

    Возвращает:
    - `True`: Ошибки нет
    - `False`: Ошибки есть (ошибки доступа, недоступности, скрытия)
    '''
    mes = soup.find('div', class_='message_page_title')
    if mes is not None:
        return False if 'Ошибка' in mes.text else True
    return True


async def find_link_by_url(session: aiohttp.ClientSession, url: str, pattern: str, cookies=None) -> str:
    '''
    Находит ссылку на файл из документа VK. Если не найдено, возвращает переданный `url`
    `session`: сессия
    `url`: ссылка на документ VK, где нужно найти ссылку
    `pattern`: паттерн для поиска
    - `doc`: паттерн для поиска ссылок в документах
    `cookies` куки для `aiohttp.ClientResponse`
    '''
    async with session.get(url, timeout=15, cookies=cookies) as response:
        assert response.status == 200, f'Response status: {response.status}'
        if 'text/html' in response.headers['content-type']:
            soup = BeautifulSoup(await response.text(), 'html.parser')
            assert check_vk_title_error(soup), 'Ошибка доступа к документу'
            if 'doc' in pattern:
                for t in ['img', 'iframe']:
                    if soup.find(t) is not None:
                        return soup.find(t).get('src')
        redirect_url = str(response.url)
        if redirect_url != url:
            return redirect_url
        return url


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


async def get_info(url: str, save_path: str, file_name: str, sema: asyncio.BoundedSemaphore, cookies=None) -> Dict[str, str] | None:
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
    `cookies` куки для `aiohttp.ClientSession`
    '''
    try:
        if 'vk.com/video' in url:
            return {'url': url, 'file_info': 'vk_video'}
        elif 'vk.com/id' in url or 'vk.com/public' in url:
            return {'url': url, 'file_info': 'vk_contact'}
        elif 'vk.com/story' in url:
            return {'url': url, 'file_info': 'vk_story'}
        elif 'github.com' in url:
            return {'url': url, 'file_info': 'github_link'}
        elif 'aliexpress.com' in url:
            return {'url': url, 'file_info': 'aliexpress_link'}
        elif 'pastebin.com' in url:
            return {'url': url, 'file_info': 'pastebin_link'}
        elif 'drive.google.com' in url:
            return {'url': url, 'file_info': 'gdrive_link'}
        elif 'google.com' in url:
            return {'url': url, 'file_info': 'google_link'}
        elif 'wikipedia.org' in url:
            return {'url': url, 'file_info': 'wikipedia_link'}
        elif 'pornhub.com' in url:
            return {'url': url, 'file_info': '🍓'}
        elif 't.me' in url:
            return {'url': url, 'file_info': 'telegram_contact'}
        elif 'dns-shop.ru' in url:
            return {'url': url, 'file_info': 'dns_shop_link'}

        async with sema, aiohttp.ClientSession(headers={'Accept-Language': 'ru'}) as session:
            async with session.get(url, timeout=45) as response:
                assert response.status == 200, f'Response status: {response.status}'
                if any(t in response.headers['content-type'] for t in ('image', 'audio')):
                    response_info = get_response_info(response.headers['content-type'])
                    download_path = join(save_path, response_info['full_type_info'])
                    tools.create_folder(download_path)
                    download_file_name = get_file_name_by_link(str(response.url))
                    if download_file_name is None:
                        download_file_name = f'{file_name}.{response_info["extension"]}'
                    await asyncio.create_task(
                        downloader(
                            response=response,
                            path=download_path,
                            name=download_file_name
                        )
                    )
                    return {'url': str(response.url), 'file_info': response_info['full_type_info']}
                target_content_type = response.headers['content-type']

            if 'text/html' in target_content_type and 'vk.com/doc' in url:
                find_res = await asyncio.create_task(find_link_by_url(session, url, 'doc', cookies))
                async with session.get(find_res, timeout=900) as response:
                    response_info = get_response_info(response.headers['content-type'])
                    download_path = join(save_path, response_info['full_type_info'])
                    tools.create_folder(download_path)
                    download_file_name = get_file_name_by_link(find_res)
                    if download_file_name is None:
                        download_file_name = f'{file_name}.{response_info["extension"]}'
                    await asyncio.create_task(
                        downloader(
                            response=response,
                            path=download_path,
                            name=download_file_name
                        )
                    )
                    return {'url': str(response.url), 'file_info': response_info['full_type_info']}

            return {'url': url, 'file_info': 'not_parse'}
    except Exception as e:
        logger.error(f'Ошибка 🔗 {url}: {e}')
        logger.debug(format_exc())
        return {'url': url, 'file_info': 'error'}
