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
    `messages_info` сырые данные для обработчика о сообщениях их `VKLinkFinder`
    `folder`: имя папки для хранения файлов

    Возвращает структурированные данные и количество обработанных ссылок
    '''
    result = {}
    full_count = 0
    for id, id_info in messages_info.items():
        logger.debug(id)
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
        tasks_result = list(filter(None, await asyncio.gather(*tasks)))
        logger.debug(f'Задачи на обработку 🔗 выполнены, количество валидных данных: {len(tasks_result)}')
        for res in tasks_result:
            file_info = result[id].setdefault(res['file_info'], [])
            file_info.append(str(res['url']))
    return result, full_count


folder_info = {
    'messages': {
        'folder': 'messages',
        'handler': messages_handler
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
    for data_dype, info in obj.link_info.items():
        logger.info(f'[===] Начат процесс обработки {data_dype} [===]')
        coroutine_handler = folder_info[data_dype]['handler']
        res_handler, count = await asyncio.create_task(coroutine_handler(info, data_dype))
        result[data_dype] = res_handler
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
