import re
from configparser import ConfigParser
from os import listdir, makedirs, remove
from os.path import exists, isdir, join
from shutil import rmtree
from typing import Generator

from logger import create_logger

config = ConfigParser()
config_read = config.read('config.ini', encoding='utf8')
if config_read is None:
    logger = create_logger('logs/vk_parser.log', 'tools', 'DEBUG')
else:
    log_level = config['main_parameters'].get('log_level', 'DEBUG')
    logger = create_logger('logs/vk_parser.log', 'tools', log_level)

forbidden_char_name = r'[^0-9a-zA-Zа-яА-ЯёЁ.\s_\\\/-]'

months_dict = {
    'янв': '1',
    'фев': '2',
    'мар': '3',
    'апр': '4',
    'мая': '5',
    'июн': '6',
    'июл': '7',
    'авг': '8',
    'сен': '9',
    'окт': '10',
    'ноя': '11',
    'дек': '12'
}


def get_numberic_date(date: str) -> str:
    '''
    Меняет буквенный месяц в дате на числовой
    `date`: дата
    '''
    day, month, year = date.split('_')
    return f'{day}_{months_dict[month]}_{year}'


def clear_charters_by_pattern(input_str: str, pattern: str = forbidden_char_name, repl_char: str = '_') -> str:
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
        logger.debug(f'Создана папка по пути {path}')


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
