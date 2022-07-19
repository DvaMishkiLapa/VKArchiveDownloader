import asyncio
import configparser
import json
import os
import traceback
from datetime import datetime
from os.path import join
from typing import Any, Dict, Tuple

import browser_cookie3
import urllib3
from requests.utils import dict_from_cookiejar

import data_downloader
import tools
from links_finder import VKLinkFinder
from logger import create_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

config = configparser.ConfigParser()
config_read = config.read('config.ini', encoding='utf8')
if config_read is None:
    logger = create_logger('logs/vk_parser.log', 'main', 'DEBUG')
else:
    log_level = config['main_parameters'].get('log_level', 'DEBUG')
    logger = create_logger('logs/vk_parser.log', 'main', log_level)

output_folder = 'output'


async def messages_handler(info: Dict[str, Any], folder: str, sema: asyncio.BoundedSemaphore, cookies=None) -> Tuple[Any]:
    '''
    Обработчик данных о сообщениях
    `info` сырые данные для обработчика о сообщениях из `VKLinkFinder`
    `folder`: имя папки для хранения файлов
    `sema`: семафор для асинхронного скачивания
    `cookies` cookies файлы авторизации VK

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    for id, id_info in info.items():
        logger.debug(f'Начата обработка 🔗 для {id}, {id_info["name"]}')
        result[id] = {'name': id_info["name"], 'dialog_link': id_info['dialog_link']}
        dialog_name_id = f'{tools.clear_spec(id_info["name"])}_{id}'
        path_for_create = join(output_folder, folder, dialog_name_id)
        tools.create_folder(path_for_create)
        logger.debug(f'Создана папка по пути {path_for_create}')
        tasks = [asyncio.ensure_future(
            data_downloader.get_info(
                url=v,
                save_path=path_for_create,
                file_name=id_info['links'].index(v),
                sema=sema,
                cookies=cookies
            )
        ) for v in id_info['links']]
        count = len(tasks)
        full_count += count
        logger.debug(f'Задачи на обработку 🔗 созданы, их количество: {count}')
        tasks_result = list(filter(lambda link: link, await asyncio.gather(*tasks)))
        logger.debug(f'Задачи на обработку 🔗 выполнены, количество валидных данных: {len(tasks_result)}')
        for res in tasks_result:
            file_info = result[id].setdefault(res['file_info'], [])
            file_info.append(res['url'])
    return result, full_count


async def likes_photo_handler(info: Dict[str, Any], folder: str, sema: asyncio.BoundedSemaphore, cookies=None) -> Tuple[Any]:
    '''
    Обработчик данных о лайкнутых фото
    `info` сырые данные для обработчика о лайкнутых фото из `VKLinkFinder`
    `folder`: имя папки для хранения файлов
    `sema`: семафор для асинхронного скачивания
    `cookies` cookies файлы авторизации VK

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    path_for_create = join(output_folder, folder)
    tasks = [asyncio.ensure_future(
        data_downloader.get_info(
            url=v,
            save_path=path_for_create,
            file_name=info['links'].index(v),
            sema=sema,
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


async def profile_photos_handler(info: Dict[str, Any], folder: str, sema: asyncio.BoundedSemaphore, cookies=None) -> Tuple[Any]:
    '''
    Обработчик данных о фото профиля
    `info` сырые данные для обработчика о фото профиля из `VKLinkFinder`
    `folder`: имя папки для хранения файлов
    `sema`: семафор для асинхронного скачивания
    `cookies` cookies файлы авторизации VK

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    for albom, albom_info in info.items():
        result[albom] = {}
        logger.debug(f'Начата обработка 🔗 для {albom}')
        path_for_create = join(output_folder, folder, albom)
        tools.create_folder(path_for_create)
        logger.debug(f'Создана папка по пути {path_for_create}')
        tasks = [asyncio.ensure_future(
            data_downloader.get_info(
                url=v,
                save_path=path_for_create,
                file_name=albom_info['links'].index(v),
                sema=sema,
                cookies=cookies
            )
        ) for v in albom_info['links']]
        count = len(tasks)
        full_count += count
        logger.debug(f'Задачи на обработку 🔗 созданы, их количество: {count}')
        tasks_result = list(filter(lambda link: link, await asyncio.gather(*tasks)))
        logger.debug(f'Задачи на обработку 🔗 выполнены, количество валидных данных: {len(tasks_result)}')
        for res in tasks_result:
            file_info = result[albom].setdefault(res['file_info'], [])
            file_info.append(res['url'])
    return result, full_count


async def profile_handler(info: Dict[str, Any], folder: str, sema: asyncio.BoundedSemaphore, cookies=None) -> Tuple[Any]:
    '''
    Обработчик данных о профиле (скорее, о документах профиля)
    `info` сырые данные для обработчика о профиле `VKLinkFinder`
    `folder`: имя папки для хранения файлов
    `sema`: семафор для асинхронного скачивания
    `cookies` cookies файлы авторизации VK

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    info_type = 'documents'

    logger.debug(f'Начата обработка 🔗 для {info_type}')
    path_for_create = join(output_folder, folder, info_type)
    tools.create_folder(path_for_create)
    logger.debug(f'Создана папка по пути {path_for_create}')
    result[info_type] = {}
    tasks = [asyncio.ensure_future(
        data_downloader.get_info(
            url=v,
            save_path=path_for_create,
            file_name=info['links'].index(v),
            sema=sema,
            cookies=cookies
        )
    ) for v in info['links']]
    count = len(tasks)
    full_count += count
    logger.debug(f'Задачи на обработку 🔗 созданы, их количество: {count}')
    tasks_result = list(filter(lambda link: link, await asyncio.gather(*tasks)))
    logger.debug(f'Задачи на обработку 🔗 выполнены, количество валидных данных: {len(tasks_result)}')
    for res in tasks_result:
        file_info = result[info_type].setdefault(res['file_info'], [])
        file_info.append(res['url'])
    return result, full_count


async def main():
    if config_read:
        logger.debug('Конфигурационный файл успешно загружен')
    else:
        logger.error('Конфигурационный файл не найден')
        exit()

    if config['main_parameters'].get('use_coockie', False):
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

    semaphore = int(config['main_parameters'].get('semaphore', 75))
    logger.info(f'Количество одновременных скачиваний файлов: {semaphore} 🚦')
    sema = asyncio.BoundedSemaphore(semaphore)

    archive_path = config['folder_parameters'].get('vk_archive_folder', 'Archive')
    logger.info(f'📁 архива VK: {archive_path}')
    messages_folder = config['folder_parameters'].get('messages_folder')
    if messages_folder is None:
        logger.info('Парсинг сообщений будет пропущен')
    else:
        logger.info(f'Парсинг сообщений будет выполнен из 📁: {messages_folder}')
    likes_folder = config['folder_parameters'].get('likes_folder')
    if likes_folder is None:
        logger.info('Парсинг лайкнутых фото будет пропущен')
    else:
        logger.info(f'Парсинг лайкнутых фото будет выполнен из 📁: {likes_folder}')
    photos_folder = config['folder_parameters'].get('photos_folder')
    if photos_folder is None:
        logger.info('Парсинг фото профиля будет пропущен')
    else:
        logger.info(f'Парсинг фото профиля будет выполнен из 📁: {photos_folder}')
    profile_folder = config['folder_parameters'].get('profile_folder')
    if profile_folder is None:
        logger.info('Парсинг документов профиля будет пропущен')
    else:
        logger.info(f'Парсинг документов профиля будет выполнен из 📁: {profile_folder}')

    folder_info = {
        'messages': {
            'folder': messages_folder,
            'handler': messages_handler
        },
        'likes_photo': {
            'folder': likes_folder,
            'handler': likes_photo_handler
        },
        'photos': {
            'folder': photos_folder,
            'handler': profile_photos_handler
        },
        'profile': {
            'folder': profile_folder,
            'handler': profile_handler
        }
    }
    folder_keys = {}
    for key, value in folder_info.items():
        if folder_info[key]['folder'] is not None:
            folder_keys.update({key: value['folder']})

    tools.clear_jsons(output_folder)
    tools.clear_folder(output_folder)
    logger.info(f'📁 {output_folder} очищена 🗑️')
    logger.info('🔥 Начат процесс получения данных из архива VK... 🔥')
    first_start = datetime.now()
    obj = VKLinkFinder(archive_path, folder_names=folder_keys, core_count=core_count)
    logger.info(f'⌛ создания файла JSON с информацией о ссылках: {datetime.now() - first_start}')
    for folder in obj.link_info.keys():
        tools.create_folder(join(output_folder, folder))
    # with open(join(output_folder, 'dirty_links.json'), 'w', encoding='utf8') as f:
    #     f.write(json.dumps(obj.link_info, indent=4, ensure_ascii=False))

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
                sema=sema,
                cookies=cookies
            )
        )
        result[data_type] = res_handler
        full_count += count
    full_end = datetime.now()

    logger.info(f'Количество обработанных 🔗: {full_count}')
    logger.info(f'⌛ обработки 🔗 и скачивания возможных: {full_end - start}')
    with open(join(output_folder, 'links_info.json'), 'w', encoding='utf8') as f:
        f.write(json.dumps(result, indent=4, ensure_ascii=False))
    logger.info(f'Общее ⌛ обработки архива VK: {full_end - first_start}')


if __name__ == '__main__':
    try:
        if 'nt' in os.name:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except Exception:
        logger.critical(traceback.format_exc())
