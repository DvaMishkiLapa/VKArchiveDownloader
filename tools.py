import os
from os.path import join, isdir
from shutil import rmtree
from typing import Generator


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
        folder_path = join(path, f)
        if isdir(folder_path):
            rmtree(folder_path)


def clear_jsons(path: str) -> None:
    '''
    Удаляет все `JSON` файлы по пути из `path`
    `path`: путь для удаления папок и файлов
    '''
    for f in listdir_nohidden(path):
        if '.json' in f:
            os.remove(join(path, f))
