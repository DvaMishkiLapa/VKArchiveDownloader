import asyncio
import json
import os
import traceback
from datetime import datetime
from os.path import join
from shutil import rmtree
from typing import Coroutine, Dict, Generator

import aiofiles
import aiohttp

from archive_parser import VKArchiveParser
from logger import create_logger

logger = create_logger('logs/vk_parser.log', 'main', 'DEBUG')
archive_path = join('Archive', 'messages')
output_folder = 'output'
downloads_folder = 'downloads'


def clear_spec(dstr: str, charters: str = '\\/:*?"<>|') -> str:
    '''
    Удаляет символы `charters` из строки `dstr`
    `dstr`: строка для очищения
    `charters`: символы, которые нужно удалить из `dstr`
    '''
    for x in charters:
        dstr = dstr.replace(x, '')
    return dstr


def create_folder(path: str) -> None:
    '''
    Создает все папки по пути из `path`
    `path`: путь для создания папок и подпапок
    '''
    if not os.path.exists(path):
        os.makedirs(path)


def listdir_nohidden(path: str) -> Generator:
    '''
    Возвращает не скрытые файлы и папки по пути `path`
    `path`: путь до необходимой папки
    '''
    for f in os.listdir(path):
        if not f.startswith('.'):
            yield f


def clear_folder(path: str) -> None:
    '''
    Удаляет все папки и файлы по пути из `path`
    `path`: путь для удаления папок и файлов
    '''
    for f in listdir_nohidden(path):
        rmtree(path, f)


def clear_jsons(path: str) -> None:
    '''
    Удаляет все `JSON` файлы по пути из `path`
    `path`: путь для удаления папок и файлов
    '''
    for f in listdir_nohidden(path):
        if 'json' in f:
            os.remove(join(path, f))


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


async def get_info(url: str, save_path: str, file_name: str) -> Dict[str, str] | None:
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
                assert response.status == 200
                file_info = response.headers['content-type'].split(';')[0]
                file_type = file_info.split('/')[-1]
                if any(t in response.headers['content-type'] for t in ('image', 'audio')):
                    download_path = os.path.join(save_path, file_type)
                    create_folder(os.path.join(download_path))
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


async def main():
    clear_folder(os.path.join(output_folder, downloads_folder))
    clear_jsons(output_folder)
    logger.info(f'📁 {output_folder} очищена 🗑️')

    logger.info('🔥 Начат процесс получения данных из архива VK... 🔥')
    first_start = datetime.now()
    obj = VKArchiveParser(archive_path)
    logger.info(f'⌛ создания файла JSON с информацией о ссылках: {datetime.now() - first_start}')
    with open(os.path.join(output_folder, 'dirty_links.json'), 'w', encoding='utf8') as f:
        f.write(json.dumps(obj.link_info, indent=4, ensure_ascii=False))

    result = {}
    full_count = 0
    start = datetime.now()
    for key, value in obj.link_info.items():
        logger.debug(f'Начата обработка 🔗 для {key}, {value["name"]}')
        result[key] = {'name': value["name"], 'dialog_link': value['dialog_link']}
        dialog_name_id = f'{clear_spec(value["name"])}_{key}'
        path_for_create = os.path.join(output_folder, downloads_folder, dialog_name_id)
        create_folder(path_for_create)
        logger.debug(f'Создана папка по пути {path_for_create}')
        tasks = [asyncio.ensure_future(get_info(v, path_for_create, value['links'].index(v))) for v in value['links']]
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
    with open(os.path.join(output_folder, 'links_info.json'), 'w', encoding='utf8') as f:
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
