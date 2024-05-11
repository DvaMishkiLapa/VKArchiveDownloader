import asyncio
import os
import ssl
from configparser import ConfigParser
from datetime import datetime
from json import dumps
from multiprocessing import freeze_support
from os.path import isdir, join
from traceback import format_exc
from typing import Any, Dict, Tuple

import aiohttp
import browser_cookie3
import urllib3
from latest_user_agents import get_random_user_agent
from requests.utils import dict_from_cookiejar

import data_downloader
import tools
from links_finder import VKLinkFinder
from logger import create_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

config = ConfigParser()
config_read = config.read('config.ini', encoding='utf8')
if config_read is None:
    logger = create_logger('logs/vk_parser.log', 'main', 'DEBUG')
else:
    log_level = config['main_parameters'].get('log_level', 'DEBUG')
    logger = create_logger('logs/vk_parser.log', 'main', log_level)

output_folder = 'output'


def get_ssl_context_tcp_connector(disable_ssl: bool = False) -> aiohttp.TCPConnector:
    ssl_context = ssl.create_default_context()
    if disable_ssl:
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
    else:
        ssl_context.check_hostname = True
        ssl_context.verify_mode = ssl.CERT_REQUIRED
    return aiohttp.TCPConnector(ssl=ssl_context)


async def messages_handler(
    info: Dict[str, Any],
    folder: str,
    cookies: None,
    sema: asyncio.BoundedSemaphore,
    disable_ssl: bool = False,
    save_by_date: bool = False
) -> Tuple[Any]:
    '''
    Обработчик данных о сообщениях
    `info` сырые данные для обработчика о сообщениях из `VKLinkFinder`
    `folder`: имя папки для хранения файлов
    `sema`: семафор для асинхронного скачивания
    `tcp_connector`: TCP коннектор, если будут изменения в работе TCP
    `cookies` cookies файлы авторизации VK

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    headers = {
        'Accept-Language': 'ru',
        'User-Agent': get_random_user_agent()
    }

    conn = get_ssl_context_tcp_connector(disable_ssl)

    async with aiohttp.ClientSession(headers=headers, connector=conn) as session:
        for id, id_info in info.items():
            count_by_id = 0
            logger.debug(f'Начата обработка 🔗 для {id}, {id_info["name"]}')
            result[id] = {'name': id_info["name"], 'dialog_link': id_info['dialog_link']}
            dialog_name_id = f'{id_info["name"]}_{id}'
            path_for_id = tools.clear_charters_by_pattern(join(output_folder, folder, dialog_name_id))
            for date, links in id_info['links'].items():
                if save_by_date:
                    storage = result[id].setdefault(date, {})
                    path_for_create = tools.clear_charters_by_pattern(join(path_for_id, date))
                else:
                    storage = result[id]
                    path_for_create = path_for_id
                tasks = [asyncio.ensure_future(
                    data_downloader.get_info(
                        url=v,
                        save_path=path_for_create,
                        file_name=links.index(v),
                        session=session,
                        semaphore=sema,
                        cookies=cookies
                    )
                ) for v in links]
                full_count += len(tasks)
                tasks_result = list(filter(lambda link: link, await asyncio.gather(*tasks)))
                count_by_id += len(tasks_result)
                for res in tasks_result:
                    file_info = storage.setdefault(res['file_info'], [])
                    file_info.append(res['url'])
            logger.debug(f'Количество валидных данных, полученных из 🔗: {count_by_id}')
    return result, full_count


async def likes_photo_handler(
    info: Dict[str, Any],
    folder: str,
    cookies: None,
    sema: asyncio.BoundedSemaphore,
    disable_ssl: bool = False,
    save_by_date: bool = False
) -> Tuple[Any]:
    '''
    Обработчик данных о лайкнутых фото
    `info` сырые данные для обработчика о лайкнутых фото из `VKLinkFinder`
    `folder`: имя папки для хранения файлов
    `sema`: семафор для асинхронного скачивания
    `tcp_connector`: TCP коннектор, если будут изменения в работе TCP
    `cookies` cookies файлы авторизации VK

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    path_for_create = join(output_folder, folder)

    headers = {
        'Accept-Language': 'ru',
        'User-Agent': get_random_user_agent()
    }

    conn = get_ssl_context_tcp_connector(disable_ssl)

    async with aiohttp.ClientSession(headers=headers, connector=conn) as session:
        tasks = [asyncio.ensure_future(
            data_downloader.get_info(
                url=v,
                save_path=path_for_create,
                file_name=info['links'].index(v),
                session=session,
                semaphore=sema,
                cookies=cookies
            )
        ) for v in info['links']]
        count = len(tasks)
        full_count += count
        logger.debug(f'Задачи на обработку 🔗 созданы, их количество: {count}')
        tasks_result = list(filter(lambda link: link, await asyncio.gather(*tasks)))
    for res in tasks_result:
        file_info = result.setdefault(res['file_info'], [])
        file_info.append(res['url'])
    return result, full_count


async def profile_photos_handler(
    info: Dict[str, Any],
    folder: str,
    cookies: None,
    sema: asyncio.BoundedSemaphore,
    disable_ssl: bool = False,
    save_by_date: bool = False
) -> Tuple[Any]:
    '''
    Обработчик данных о фото профиля
    `info` сырые данные для обработчика о фото профиля из `VKLinkFinder`
    `folder`: имя папки для хранения файлов
    `sema`: семафор для асинхронного скачивания
    `tcp_connector`: TCP коннектор, если будут изменения в работе TCP
    `cookies` cookies файлы авторизации VK
    `save_by_date`: сохранять ли файлы в подпапки на основе даты

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    headers = {
        'Accept-Language': 'ru',
        'User-Agent': get_random_user_agent()
    }

    conn = get_ssl_context_tcp_connector(disable_ssl)

    async with aiohttp.ClientSession(headers=headers, connector=conn) as session:
        for albom, albom_info in info.items():
            logger.debug(f'Начата обработка 🔗 для {albom}')
            result[albom] = {}
            count_by_albom = 0
            path_for_albom = tools.clear_charters_by_pattern(join(output_folder, folder, albom))
            for date, links in albom_info.items():
                if save_by_date:
                    storage = result[albom].setdefault(date, {})
                    path_for_create = tools.clear_charters_by_pattern(join(path_for_albom, date))
                else:
                    storage = result[albom]
                    path_for_create = path_for_albom
                tasks = [asyncio.ensure_future(
                    data_downloader.get_info(
                        url=v,
                        save_path=path_for_create,
                        file_name=links.index(v),
                        session=session,
                        semaphore=sema,
                        cookies=cookies
                    )
                ) for v in links]
                full_count += len(tasks)
                tasks_result = list(filter(lambda link: link, await asyncio.gather(*tasks)))
                count_by_albom += len(tasks_result)
                for res in tasks_result:
                    file_info = storage.setdefault(res['file_info'], [])
                    file_info.append(res['url'])
            logger.debug(f'Количество валидных данных, полученных из 🔗: {count_by_albom}')
    return result, full_count


async def profile_handler(
    info: Dict[str, Any],
    folder: str,
    cookies: None,
    sema: asyncio.BoundedSemaphore,
    disable_ssl: bool = False,
    save_by_date: bool = False
) -> Tuple[Any]:
    '''
    Обработчик данных о профиле (скорее, о документах профиля)
    `info` сырые данные для обработчика о профиле `VKLinkFinder`
    `folder`: имя папки для хранения файлов
    `sema`: семафор для асинхронного скачивания
    `tcp_connector`: TCP коннектор, если будут изменения в работе TCP
    `cookies` cookies файлы авторизации VK

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    count_by_doc = 0
    info_type = 'documents'
    logger.debug(f'Начата обработка 🔗 для {info_type}')
    path_for_doc = tools.clear_charters_by_pattern(join(output_folder, folder, info_type))

    headers = {
        'Accept-Language': 'ru',
        'User-Agent': get_random_user_agent()
    }

    conn = get_ssl_context_tcp_connector(disable_ssl)

    async with aiohttp.ClientSession(headers=headers, connector=conn) as session:
        for date, links in info.items():
            if save_by_date:
                storage = result.setdefault(date, {})
                path_for_create = tools.clear_charters_by_pattern(join(path_for_doc, date))
            else:
                storage = result
                path_for_create = path_for_doc
            tasks = [asyncio.ensure_future(
                data_downloader.get_info(
                    url=v,
                    save_path=path_for_create,
                    file_name=links.index(v),
                    session=session,
                    semaphore=sema,
                    cookies=cookies
                )
            ) for v in links]
            full_count += len(tasks)
            tasks_result = list(filter(lambda link: link, await asyncio.gather(*tasks)))
            count_by_doc += len(tasks_result)
            for res in tasks_result:
                file_info = storage.setdefault(res['file_info'], [])
                file_info.append(res['url'])
    logger.debug(f'Количество валидных данных, полученных из 🔗: {count_by_doc}')
    return result, full_count


def folder_check(
    folder_name: str,
    human_folder_name: str,
    archive_path: str
) -> str | None:
    '''
    Проверка папки с данными из архива на указание парсинга и существование, отправляя логи о результате
    `folder_name`: имя папки для проверки
    `human_folder_name`: описательное имя папки в винительном падеже (кого? что?)
    `archive_path`: путь до папки архива
    '''
    folder = config['folder_parameters'].get(folder_name)
    if folder is None:
        logger.info(f'Парсинг {human_folder_name} будет пропущен')
    elif isdir(join(archive_path, folder)):
        logger.info(f'Парсинг {human_folder_name} будет выполнен из 📁: {folder}')
    else:
        logger.warning(f'📁 для парсинга {human_folder_name} указана, но не найдена: {folder}')
        folder = None
    return folder


async def main():
    if config_read:
        logger.debug('Конфигурационный файл успешно загружен')
    else:
        logger.error('Конфигурационный файл не найден')
        exit()

    if config['main_parameters'].getboolean('use_coockie', False):
        cookies = dict_from_cookiejar(browser_cookie3.load(domain_name='vk.com'))
        logger.info('В работе утилиты будут использованы файлы 🍪')
    else:
        cookies = None
        logger.info('В работе утилиты НЕ будут использованы файлы 🍪')

    core_count = int(config['main_parameters'].get('core_count', 0))
    if core_count:
        logger.info(f'Используемое количество потоков: {core_count} 🚀')
    else:
        logger.info('Используемое количество потоков будет определено автоматически 🚀')

    semaphore_small = int(config['main_parameters'].get('semaphore_small', 75))
    logger.info(f'Количество одновременных скачиваний файлов малого размера: {semaphore_small} 🚦')
    sema_small = asyncio.BoundedSemaphore(semaphore_small)

    semaphore_big = int(config['main_parameters'].get('semaphore_big', 10))
    logger.info(f'Количество одновременных скачиваний файлов большого размера: {semaphore_big} 🚦')
    sema_big = asyncio.BoundedSemaphore(semaphore_big)

    archive_path = config['folder_parameters'].get('vk_archive_folder', 'Archive')
    logger.info(f'📁 архива VK: {archive_path}')

    messages_folder = folder_check(
        folder_name='messages_folder',
        human_folder_name='сообщений',
        archive_path=archive_path
    )
    likes_folder = folder_check(
        folder_name='likes_folder',
        human_folder_name='лайкнутых фото',
        archive_path=archive_path
    )
    photos_folder = folder_check(
        folder_name='photos_folder',
        human_folder_name='фото профиля',
        archive_path=archive_path
    )
    profile_folder = folder_check(
        folder_name='profile_folder',
        human_folder_name='документов профиля',
        archive_path=archive_path
    )

    folder_info = {
        'messages': {
            'folder': messages_folder,
            'handler': messages_handler,
            'semaphore': sema_small
        },
        'likes/photo': {
            'folder': likes_folder,
            'handler': likes_photo_handler,
            'semaphore': sema_big
        },
        'photos': {
            'folder': photos_folder,
            'handler': profile_photos_handler,
            'semaphore': sema_small
        },
        'profile': {
            'folder': profile_folder,
            'handler': profile_handler,
            'semaphore': sema_big
        }
    }
    folder_keys = {}
    for key, value in folder_info.items():
        if folder_info[key]['folder'] is not None:
            folder_keys.update({key: value['folder']})

    delete_output_folder = config['main_parameters'].getboolean('delete_output_folder', False)
    if delete_output_folder:
        logger.info(f'📁 {output_folder} будет отчищена перед началом работы 🗑️')
        tools.clear_folder(output_folder)
    else:
        logger.info(f'📁 {output_folder} останется нетронутой')

    save_by_date = config['main_parameters'].getboolean('save_by_date', False)
    if save_by_date:
        logger.info('Файлы будут сохранены в подпапки, на основе информации о дате')
    else:
        logger.info('Сохранение файлов в подпапки на основе информации о дате не будет использоваться')

    logger.info('🔥 Начат процесс получения данных из архива VK... 🔥')
    first_start = datetime.now()
    obj = VKLinkFinder(archive_path, folder_names=folder_keys, core_count=core_count)
    logger.info(f'⌛ создания файла JSON с информацией о ссылках: {datetime.now() - first_start}')
    for folder in obj.link_info.keys():
        tools.create_folder(join(output_folder, folder))

    if 'DEBUG' in log_level:
        with open(join(output_folder, 'dirty_links.json'), 'w', encoding='utf8') as f:
            f.write(dumps(obj.link_info, indent=4, ensure_ascii=False))

    disable_ssl = config['main_parameters'].getboolean('disable_ssl', False)

    result = {}
    full_count = 0
    start = datetime.now()
    for data_type, info in obj.link_info.items():
        logger.info(f'⚙️ Начат процесс обработки {data_type} ⚙️')
        coroutine_handler = folder_info[data_type]['handler']
        res_handler, count = await asyncio.create_task(
            coroutine_handler(
                info=info,
                folder=folder_info[data_type]['folder'],
                sema=folder_info[data_type]['semaphore'],
                cookies=cookies,
                disable_ssl=disable_ssl,
                save_by_date=save_by_date
            )
        )
        result[data_type] = res_handler
        full_count += count
    full_end = datetime.now()

    logger.info(f'Количество обработанных 🔗: {full_count}')
    logger.info(f'⌛ обработки 🔗 и скачивания возможных: {full_end - start}')
    with open(join(output_folder, 'links_info.json'), 'w', encoding='utf8') as f:
        f.write(dumps(result, indent=4, ensure_ascii=False))
    logger.info(f'Общее ⌛ обработки архива VK: {full_end - first_start}')


if __name__ == '__main__':
    freeze_support()
    try:
        if 'nt' in os.name:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except Exception:
        logger.critical(format_exc())
