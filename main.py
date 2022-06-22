import json
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from itertools import chain
from os import cpu_count, listdir
from os.path import isdir, isfile, join, split, splitext

# import requests
from bs4 import BeautifulSoup

vk_url = 'https://vk.com/'

archive_path = join('Archive', 'messages')

vk_encoding = 'cp1251'
normal_encoding = 'utf8'

cpu_count = cpu_count()
cpu_count = 1 if cpu_count is None else cpu_count


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


def get_dialog_type(dialog_path: str) -> list:
    '''
    Возвращает тип диалога и его ID (без -, если был):
    - `id`: личная беседа
    - `public`: общая беседа
    `dialog_path`: путь до папки диалога
    '''
    folder_name = split(dialog_path)[-1]
    dialog_type = 'public' if '-' in folder_name else 'id'
    dialog_id = folder_name.replace('-', '')
    return dialog_type, dialog_id


def get_vk_attachments(base_dir: str) -> dict:
    '''
    Возвращает информацию о всех вложения в VK архиве
    `base_dir`: путь до папки VK архива
    '''
    result = {}
    dirs = get_all_dirs_from_directory(base_dir)
    for path in dirs:
        find_links = walk_dialog_directory(path)
        dialog_type, dialog_id = get_dialog_type(path)
        dialog_full_id = f'{dialog_type}{dialog_id}'
        result[dialog_id] = {
            'name': hook_dialog_name(path),
            'dialog_link': f'{vk_url}{dialog_full_id}',
            'links': find_links
        }
    return result


if __name__ == '__main__':
    start = datetime.now()
    res = get_vk_attachments(archive_path)
    print(f'Время выполнения: {datetime.now() - start}')
    with open('links.json', 'w', encoding=normal_encoding) as f:
        f.write(json.dumps(res, indent=4, ensure_ascii=False))
