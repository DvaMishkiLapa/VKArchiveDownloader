import json
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from itertools import chain
from os import listdir, sep
from os.path import basename, dirname, isdir, isfile, join, splitext

# import requests
from bs4 import BeautifulSoup

archive_path = 'Archive/messages'

vk_encoding = 'cp1251'
normal_encoding = 'utf8'

cpu_count = os.cpu_count()
cpu_count = 1 if cpu_count is None else cpu_count

all_file_type = set()


def get_attachment(file_path: str) -> list:
    '''
    Возвращает все ссылки на вложения из `html` файла
    `file_path`: путь до файла для чтения
    '''
    with open(file_path, encoding=vk_encoding) as f:
        try:
            soup = BeautifulSoup(f.read(), 'html.parser')
            link_tags = soup.find_all('a', class_='attachment__link')
            return [tag['href'] for tag in link_tags]
        except Exception as e:
            print(f'Error in file {file_path}: {e}')


def get_all_files_from_directory(path: str, ext: list) -> list:
    '''
    Возвращает пути до всех файлов, которые содержатся в папке
    `path`: путь до необходимой папки
    `ext`: какие типы файлов необходимы (расширения файлов)
    '''
    return [join(path, f) for f in listdir(path) if isfile(join(path, f)) and splitext(join(path, f))[1] in ext]


def walk_dialog_directory(dir_path: str) -> list:
    '''
    Возвращает все вложения из папки диалога
    `dir_path`: путь до папки диалога
    '''
    files = get_all_files_from_directory(dir_path, ['.html'])
    result = []
    with ProcessPoolExecutor(cpu_count) as executor:
        result = list(chain(*executor.map(get_attachment, files)))
    return result


def get_all_dirs_from_directory(path: str) -> list:
    '''
    Возвращает путь до всех папок, находящиеся в нужной папке
    `path`: путь до нужной папки
    '''
    return [join(path, f) for f in listdir(path) if isdir(join(path, f))]


def hook_dialog_name(path: str) -> str | None:
    '''
    Находит имя человека с которым велся диалог (или название диалога/беседы)
    Если имя не будет найдено, вернет `None`
    path`: путь до папки диалога
    '''
    with open(join(path, 'messages0.html'), 'r', encoding=vk_encoding) as f:
        html = f.read()
        soup = BeautifulSoup(html, 'html.parser')
        name = soup.find('div', class_='ui_crumb')
    return None if name is None else str(name.string)


def get_dialog_type(dialog_path: str) -> str:
    '''
    Возвращает тип диалога:
    - `id`: личная беседа
    - `public`: общая беседа
    `dialog_path`: путь до папки диалога
    '''
    folder_name = os.path.split(dialog_path)[-1]
    return 'public' if '-' in folder_name else 'id'


def get_vk_attachments(base_dir: str) -> dict:
    '''
    Возвращает информацию о всех вложения в VK архиве
    `base_dir`: путь до папки VK архива
    '''
    result = {}
    dirs = get_all_dirs_from_directory(base_dir)
    for path in dirs:
        name = hook_dialog_name(path)
        find_links = walk_dialog_directory(path)
        # if len(find_links) > 0:
        folder_name = basename(dirname(path + sep))
        dialog_type = get_dialog_type(path)
        dialog_id = f'{dialog_type}{folder_name}'.replace('-', '')
        result[dialog_id] = {
            'name': hook_dialog_name(path),
            'dialog_link': f'https://vk.com/{dialog_id}',
            'links': find_links
        }
    return result


if __name__ == '__main__':
    first_start = datetime.now()
    for x in range(3):
        start = datetime.now()
        res = get_vk_attachments(archive_path)
        print(f'Прогон {x}: {datetime.now() - start}')
    first_end = datetime.now()
    print(f'Среднее время выполнения: {(first_end - first_start) / 3}')
    with open('links.json', 'w', encoding=normal_encoding) as f:
        f.write(json.dumps(res, indent=4, ensure_ascii=False))
