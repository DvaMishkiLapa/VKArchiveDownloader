import asyncio
import json
import os
import traceback
from datetime import datetime
from os.path import join

import data_downloader
import tools
from links_finder import VKLinkFinder
from logger import create_logger

logger = create_logger('logs/vk_parser.log', 'main', 'DEBUG')

archive_path = join('Archive', 'messages')
output_folder = 'output'
downloads_folder = 'downloads'

mes_folder = 'messages'


async def main():
    core_count = os.cpu_count()
    if core_count is None:
        core_count = 1
        thread_download_flag = False
        logger.warning('Не удалось получить число логических ядер процессора, получение 🔗 будет выполнено в однопоточном режиме')
        logger.info('Для скачивания файлов будет использован асинхронный режим')
    elif core_count <= 2:
        thread_download_flag = False
        # logger.info(f'Количество потоков, используемых для получение 🔗: {core_count}')
        logger.info('Процессор имеет мало ядер, для скачивания файлов будет использован асинхронный режим')
    else:
        thread_download_flag = True
        # logger.info(f'Количество потоков, используемых для получение 🔗: {core_count}')
        logger.info('Для скачивания файлов будет использован многопоточный режим режим')

    tools.clear_folder(join(output_folder, downloads_folder))
    tools.create_folder(join(output_folder, downloads_folder, mes_folder))
    tools.clear_jsons(output_folder)
    logger.info(f'📁 {output_folder} очищена 🗑️')

    logger.info('🔥 Начат процесс получения данных из архива VK... 🔥')
    first_start = datetime.now()
    obj = VKLinkFinder(archive_path)
    logger.info(f'⌛ создания файла JSON с информацией о ссылках: {datetime.now() - first_start}')
    with open(join(output_folder, 'dirty_links.json'), 'w', encoding='utf8') as f:
        f.write(json.dumps(obj.link_info, indent=4, ensure_ascii=False))

    result = {}
    full_count = 0
    start = datetime.now()
    for key, value in obj.link_info.items():
        logger.debug(f'Начата обработка 🔗 для {key}, {value["name"]}')
        result[key] = {'name': value["name"], 'dialog_link': value['dialog_link']}
        dialog_name_id = f'{tools.clear_spec(value["name"])}_{key}'
        path_for_create = join(output_folder, downloads_folder, mes_folder, dialog_name_id)
        tools.create_folder(path_for_create)
        logger.debug(f'Создана папка по пути {path_for_create}')

        if thread_download_flag:
            count = len(value['links'])
            full_count += count
            logger.debug(f'Задачи на обработку 🔗 созданы, их количество: {count}')
            tasks_result = list(filter(None, data_downloader.multi_get_executor(
                urls=value['links'],
                save_path=path_for_create,
                core_count=core_count
            )))
            logger.debug(f'Задачи на обработку 🔗 выполнены, количество валидных данных: {len(tasks_result)}')
        else:
            tasks = [asyncio.ensure_future(
                data_downloader.get_info(
                    url=v,
                    save_path=path_for_create,
                    file_name=value['links'].index(v),
                    sema=sema
                )
            ) for v in value['links']]
            count = len(tasks)
            full_count += count
            logger.debug(f'Задачи на обработку 🔗 созданы, их количество: {count}')
            tasks_result = list(filter(None, await asyncio.gather(*tasks)))
            logger.debug(f'Задачи на обработку 🔗 выполнены, количество валидных данных: {len(tasks_result)}')
        for res in tasks_result:
            file_info = result[key].setdefault(res['file_info'], [])
            file_info.append(str(res['url']))
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
