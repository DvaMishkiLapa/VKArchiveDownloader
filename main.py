import asyncio
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
cookies = dict_from_cookiejar(browser_cookie3.chrome(domain_name='vk.com'))

logger = create_logger('logs/vk_parser.log', 'main', 'DEBUG')

archive_path = 'Archive'
output_folder = 'output'


async def messages_handler(messages_info: Dict[str, Any], folder: str) -> Tuple[Any]:
    '''
    Обработчик данных о сообщениях
    `messages_info` сырые данные для обработчика о сообщениях из `VKLinkFinder`
    `folder`: имя папки для хранения файлов

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    for id, id_info in messages_info.items():
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


async def likes_photo_handler(likes_photo_info: Dict[str, Any], folder: str) -> Tuple[Any]:
    '''
    Обработчик данных о лайкнутых фото
    `likes_photo_info` сырые данные для обработчика о лайкнутых фото из `VKLinkFinder`
    `folder`: имя папки для хранения файлов

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    path_for_create = join(output_folder, folder)
    tasks = [asyncio.ensure_future(
        data_downloader.get_info(
            url=v,
            save_path=path_for_create,
            file_name=likes_photo_info['links'].index(v),
            sema=sema,
            cookies=cookies
        )
    ) for v in likes_photo_info['links']]
    count = len(tasks)
    full_count += count
    logger.debug(f'Задачи на обработку 🔗 созданы, их количество: {count}')
    tasks_result = list(filter(lambda link: link, await asyncio.gather(*tasks)))
    for res in tasks_result:
        file_info = result.setdefault(res['file_info'], [])
        file_info.append(res['url'])
    return result, full_count


async def profile_photos_handler(profile_photos_info: Dict[str, Any], folder: str) -> Tuple[Any]:
    '''
    Обработчик данных о фото профиля
    `profile_photos_info` сырые данные для обработчика о фото профиля из `VKLinkFinder`
    `folder`: имя папки для хранения файлов

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    path_for_create = join(output_folder, folder)
    tools.create_folder(path_for_create)
    logger.debug(f'Создана папка по пути {path_for_create}')
    result = {}
    tasks = [asyncio.ensure_future(
        data_downloader.get_info(
            url=v,
            save_path=path_for_create,
            file_name=profile_photos_info['links'].index(v),
            sema=sema,
            cookies=cookies
        )
    ) for v in profile_photos_info['links']]
    count = len(tasks)
    full_count += count
    logger.debug(f'Задачи на обработку 🔗 созданы, их количество: {count}')
    tasks_result = list(filter(lambda link: link, await asyncio.gather(*tasks)))
    logger.debug(f'Задачи на обработку 🔗 выполнены, количество валидных данных: {len(tasks_result)}')
    for res in tasks_result:
        file_info = result.setdefault(res['file_info'], [])
        file_info.append(res['url'])
    return result, full_count


async def profile_handler(profile_info: Dict[str, Any], folder: str) -> Tuple[Any]:
    '''
    Обработчик данных о профиле (скорее, о документах профиля)
    `profile_photos_info` сырые данные для обработчика о профиле `VKLinkFinder`
    `folder`: имя папки для хранения файлов

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    # for info_type, info in profile_info.items():
    info_type = 'documents'
    info = profile_info

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


folder_info = {
    'messages': {
        'folder': 'messages',
        'handler': messages_handler
    },
    'likes_photo': {
        'folder': join('likes', 'photo'),
        'handler': likes_photo_handler
    },
    'photos': {
        'folder': 'photos',
        'handler': profile_photos_handler
    },
    'profile': {
        'folder': 'profile',
        'handler': profile_handler
    }
}


async def main():
    tools.clear_jsons(output_folder)
    tools.clear_folder(output_folder)
    logger.info(f'📁 {output_folder} очищена 🗑️')
    logger.info('🔥 Начат процесс получения данных из архива VK... 🔥')
    first_start = datetime.now()
    folder_names = {key: values['folder'] for key, values in folder_info.items()}
    obj = VKLinkFinder(archive_path, folder_names=folder_names)
    logger.info(f'⌛ создания файла JSON с информацией о ссылках: {datetime.now() - first_start}')
    for folder in obj.link_info.keys():
        tools.create_folder(join(output_folder, folder))
    with open(join(output_folder, 'dirty_links.json'), 'w', encoding='utf8') as f:
        f.write(json.dumps(obj.link_info, indent=4, ensure_ascii=False))

    result = {}
    full_count = 0
    start = datetime.now()
    for data_type, info in obj.link_info.items():
        logger.info(f'⚙️ Начат процесс обработки {data_type} ⚙️')
        coroutine_handler = folder_info[data_type]['handler']
        res_handler, count = await asyncio.create_task(coroutine_handler(info, folder_info[data_type]['folder']))
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
        sema = asyncio.BoundedSemaphore(100)
        if 'nt' in os.name:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except Exception:
        logger.critical(traceback.format_exc())
