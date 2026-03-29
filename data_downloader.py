import asyncio
import ssl
from configparser import ConfigParser
from os.path import join
from traceback import format_exc
from typing import Coroutine, Dict

import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from latest_user_agents import get_random_user_agent

import tools
from logger import create_logger

config = ConfigParser()
config_read = config.read('config.ini', encoding='utf8')
log_level = 'DEBUG'
if config_read:
    log_level = config['main_parameters'].get('log_level', 'DEBUG')
logger = create_logger('logs/vk_parser.log', 'data_downloader', log_level)

error_titles = [
    'Ошибка | ВКонтакте',
    'Ошибка',
    'Error | VK',
    'Error'
]

error_titles_html = [f'<title>{x}</title>' for x in error_titles]


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
    '''
    Возвращает имя файла по его ссылке, если возможно
    `link`: ссылка
    '''
    try:
        if '?extra=' in link:
            return link.split('/')[-1].split('?extra=')[0]
        elif '?size=' in link:
            return link.split('/')[-1].split('?size=')[0]
        else:
            return link.split('/')[-1]
    except Exception:
        return None


def check_vk_title_error(soup: BeautifulSoup) -> str | bool:
    '''
    Проверяет наличие ошибки доступа к содержимому VK
    `soup`: экземпляр `BeautifulSoup` для работы с `HTML` контентом

    Возвращает:
    - `True`: Ошибки нет
    - `False`: Ошибки есть (ошибки доступа, недоступности, скрытия)
    '''
    mes = soup.find('div', class_='message_page_title')
    if mes is not None:
        if any(ext in mes.text for ext in error_titles):
            details = soup.find('div', class_='message_page_body').text.strip().split('\n')[0]
            return details
    return False


async def find_link_by_url(url: str, pattern: str, cookies=None, response=None, session: aiohttp.ClientSession = None) -> str:
    '''
    Находит ссылку на файл из документа VK. Если не найдено, возвращает переданный `url`
    `url`: ссылка на документ VK, где нужно найти ссылку
    `pattern`: паттерн для поиска
    `cookies` куки для `aiohttp.ClientResponse`
    `response`: ответ от первого запроса, если нужен
    `session`: сессия ClientSession, если нужна
    '''
    if 'doc' in pattern:
        async with session.get(url, timeout=60, cookies=cookies) as response:
            doc_url_pattern = 'docUrl":"'
            doc_buy_pattern = '","docBuyLink'
            assert response.status == 200, f'Response status: {response.status}'
            if 'text/html' in response.headers['content-type']:
                text = await response.text()
                assert not any(ext in text for ext in error_titles_html), 'Ошибка доступа к документу'
                first = text.find(doc_url_pattern)
                second = text.find(doc_buy_pattern)
                return text[first + len(doc_url_pattern):second].replace('\/', '/')
    elif 'photo' in pattern:
        soup = BeautifulSoup(await response.text(), 'html.parser')
        check = check_vk_title_error(soup)
        assert not check, f'Ошибка доступа к фото: {check}'
        link = soup.find('meta', attrs={'name': 'og:image:secure_url'})
        if link:
            return link['value']
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
    tools.create_folder(path)
    block_size = 16384
    async with aiofiles.open(join(path, name), 'wb') as f:
        while True:
            data = await response.content.read(block_size)
            if not data:
                break
            await f.write(data)


def links_filter(url: str) -> Dict[str, str] | None:
    if 'vk.com/video' in url:
        return {'url': url, 'file_info': 'vk_video'}
    elif 'vk.com/id' in url or 'vk.com/public' in url:
        return {'url': url, 'file_info': 'vk_contact'}
    elif 'vk.com/story' in url:
        return {'url': url, 'file_info': 'vk_story'}
    elif 'vk.com/club' in url:
        return {'url': url, 'file_info': 'vk_club'}
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
    elif 'habr.com' in url:
        return {'url': url, 'file_info': 'habr_link'}
    elif 'goo.gl' in url:
        return {'url': url, 'file_info': 'google_link'}
    elif 'onedrive.live.com' in url:
        return {'url': url, 'file_info': 'onedrive_link'}
    elif 'onedrive.com' in url:
        return {'url': url, 'file_info': 'onedrive_link'}
    elif 'youtube.com' in url:
        return {'url': url, 'file_info': 'youtube_link'}
    elif 'vk.com/login' in url:
        return {}


async def get_info(
    url: str,
    save_path: str,
    file_name: str,
    session: aiohttp.ClientSession,
    semaphore: asyncio.BoundedSemaphore,
    cookies=None
) -> Dict[str, str] | None:
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
    `session`: сессия ClientSession
    `cookies` куки для `aiohttp.ClientSession`
    `sleep`: нужна ли задержка перед выполнением цикла, если да - какая в секундах
    '''
    try:
        filter_result = links_filter(url)
        if isinstance(filter_result, dict):
            return filter_result

        async with semaphore:
            async with session.get(url, timeout=45) as response:
                assert response.status == 200, f'Response status: {response.status}'
                if any(t in response.headers['content-type'] for t in ('image', 'audio')):
                    response_info = get_response_info(response.headers['content-type'])
                    type_folder = response_info['full_type_info'].replace('/', '_')
                    download_path = tools.clear_charters_by_pattern(join(save_path, type_folder))
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

                if 'text/html' in target_content_type:
                    if 'vk.com/doc' in url:
                        find_res = await asyncio.create_task(find_link_by_url(
                            url=url,
                            pattern='doc',
                            session=session,
                            cookies=cookies
                        ))
                    elif 'vk.com/photo' in url:
                        find_res = await asyncio.create_task(find_link_by_url(
                            url=url,
                            pattern='photo',
                            response=response,
                            cookies=cookies
                        ))
                    else:
                        return {'url': url, 'file_info': 'not_parse'}
                    if find_res == url:
                        return {'url': find_res, 'file_info': 'not_parse'}
            async with session.get(find_res, timeout=900) as response:
                filter_result = links_filter(find_res)
                if isinstance(filter_result, dict):
                    return filter_result
                response_info = get_response_info(response.headers['content-type'])
                type_folder = response_info['full_type_info'].replace('/', '_')
                download_path = tools.clear_charters_by_pattern(join(save_path, type_folder))
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

    except asyncio.TimeoutError as e:
        logger.error(f'Ошибка 🔗 {url}: тайм-аут скачивания')
        logger.debug(format_exc())
        return {'url': url, 'file_info': 'timeout_error'}
    except Exception as e:
        logger.error(f'Ошибка 🔗 {url}: {e}')
        logger.debug(format_exc())
        return {'url': url, 'file_info': 'error'}
