from os import listdir, makedirs, remove
from os.path import exists, isdir, join
from shutil import rmtree
from typing import Generator
import re


def clear_charters_by_pattern(input_str: str, pattern: str = r'[^0-9a-zA-Zа-яА-ЯёЁ.]+', repl_char: str = '_') -> str:
    '''
    Удаляет символы, удоволетворяющие регулярному выражению `pattern` из строки `input_str` на символ `repl_char`
    `input_str`: строка для очищения
    `pattern`: регулярное выражение для поиска удаляемых символов
    `repl_char`: символ, на который будет заменяется символ
    '''
    return re.sub(pattern, repl_char, input_str)


def create_folder(path: str) -> None:
    '''
    Создает все папки по пути из `path`
    `path`: путь для создания папок и подпапок
    '''
    if not exists(path):
        makedirs(path)


def listdir_nohidden(path: str) -> Generator:
    '''
    Возвращает не скрытые файлы и папки по пути `path`
    `path`: путь до необходимой папки
    '''
    for f in listdir(path):
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
            remove(join(path, f))
