from concurrent.futures import ProcessPoolExecutor
from configparser import ConfigParser
from itertools import chain
from os import cpu_count, listdir
from os.path import isdir, isfile, join, split, splitext
from typing import Callable, Dict, Iterator, List, Tuple

from bs4 import BeautifulSoup

import tools
from logger import create_logger

config = ConfigParser()
config_read = config.read('config.ini', encoding='utf8')
log_level = 'DEBUG'
if config_read:
    log_level = config['main_parameters'].get('log_level', 'DEBUG')
logger = create_logger('logs/vk_parser.log', 'links_finder', log_level)


class VKLinkFinder():
    def __init__(
        self,
        archive_path: str,
        folder_names: Dict[str, str],
        vk_url: str = 'https://vk.com/',
        vk_encoding: str = 'cp1251',
        core_count: int = 0
    ) -> None:
        '''
        Парсер архива VKontakte.
        `archive_path`: Путь до архива
        `folder_names` словарь папок
        `vk_url`: ссылка на VK. Обычно, `https://vk.com/`
        `vk_encoding`: Кодировка `.html` файлов VK. Обычно, `cp1251`
        `core_count`: число потоков для многопоточной работы

        Возвращает информацию обо всех найденных ссылках в архиве
        ```
        {
            ...
            profile_id: {
                'name': 'Имя профиля или беседы'
                'dialog_link': 'Ссылка на диалог или беседу',
                'links': ['Список ссылок']
            },
            ...
        }
        ```
        '''
        self.archive_path = archive_path
        self.vk_url = vk_url
        self.vk_encoding = vk_encoding
        self.folder_names = folder_names
        if core_count <= 0:
            self.core_count = cpu_count()
            if self.core_count is None:
                self.core_count = 1
                logger.warning('Не удалось получить число логических ядер процессора, получение 🔗 будет выполнено в однопоточном режиме')
            else:
                logger.info(f'Количество потоков, используемых для получение 🔗: {self.core_count}')
        else:
            self.core_count = core_count
            logger.info(f'Количество потоков, используемых для получение 🔗: {self.core_count}')

        self.link_info = self.__get_vk_attachments()

    @classmethod
    def get_messages_attachment(self, file_path: str, vk_encoding: str = 'cp1251') -> Dict[str, List[str]] | None:
        '''
        Возвращает все ссылки на вложения из `html` файла сообщений
        `file_path`: путь до файла для чтения
        `vk_encoding`: Кодировка `.html` файлов VK. Обычно, `cp1251`
        '''
        with open(file_path, encoding=vk_encoding) as f:
            try:
                messages_info = {}
                soup = BeautifulSoup(f.read(), 'html.parser')
                messages = soup.find_all('div', class_='item__main')
                if messages:
                    for mes in messages:
                        link = mes.find('a', class_='attachment__link')
                        if link:
                            date = mes.find('div', class_='message__header')
                            if date:
                                date = date.text.strip()
                                date = '_'.join(date[date.rfind(', ') + 1:].split(' ')[1:4])
                            else:
                                date = 'no_date'
                            link_storage = messages_info.setdefault(date, [])
                            link_storage.append(link['href'])
                    return messages_info
                return ''
            except Exception as e:
                logger.error(f'Ошибка в файле {file_path}: {e}. Он будет пропущен.')
                return ''

    @classmethod
    def get_photos_attachment(self, file_path: str, vk_encoding: str = 'cp1251') -> Dict[str, list] | None:
        '''
        Возвращает все ссылки на вложения из `html` файла фото профиля
        `file_path`: путь до файла для чтения
        `vk_encoding`: Кодировка `.html` файлов VK. Обычно, `cp1251`
        '''
        with open(file_path, encoding=vk_encoding) as f:
            try:
                html_content = f.read()
                soup = BeautifulSoup(html_content, 'html.parser')
                items = soup.find_all('div', class_='item')
                if items:
                    albom_name = self.hook_albom_name(html_content)
                    result = {albom_name: {}}
                    for item in items:
                        find_link = item.find('img')['src']
                        if 'http' in find_link:
                            date = item.find('div', class_='clear_fix')
                            if date:
                                date = date.text.strip()
                                date = '_'.join(date.split(' ')[:-2])
                            else:
                                date = 'no_date'
                            links_box = result[albom_name].setdefault(date, [])
                            links_box.append(find_link)
                    return result
                return ''
            except Exception as e:
                logger.error(f'Ошибка в файле {file_path}: {e}. Он будет пропущен.')
                return ''

    @classmethod
    def get_doc_attachment(self, file_path: str, vk_encoding: str = 'cp1251') -> List[str] | None:
        '''
        Возвращает все ссылки на вложения из `html` файла документов профиля
        `file_path`: путь до файла для чтения
        `vk_encoding`: Кодировка `.html` файлов VK. Обычно, `cp1251`
        '''
        with open(file_path, encoding=vk_encoding) as f:
            try:
                doc_info = {}
                soup = BeautifulSoup(f.read(), 'html.parser')
                items = soup.find_all('div', class_='item')
                if items:
                    for el in items:
                        link = el.find('a', href=str)
                        if link:
                            date = el.find('div', class_='item__tertiary')
                            if date:
                                date = date.text.strip()
                                date = '_'.join(date.replace('\n', ' ').split(' ')[0:3])
                            else:
                                date = 'no_date'
                            link_storage = doc_info.setdefault(date, [])
                            link_storage.append(link['href'])
                    return doc_info
                return ''
            except Exception as e:
                logger.error(f'Ошибка в файле {file_path}: {e}. Он будет пропущен.')
                return ''

    @classmethod
    def get_likes_attachment(self, file_path: str, vk_encoding: str = 'cp1251') -> List[str] | None:
        '''
        Возвращает все ссылки на вложения из `html` файла лайкнутых фото профиля
        `file_path`: путь до файла для чтения
        `vk_encoding`: Кодировка `.html` файлов VK. Обычно, `cp1251`
        '''
        with open(file_path, encoding=vk_encoding) as f:
            try:
                soup = BeautifulSoup(f.read(), 'html.parser')
                link_tags = soup.find_all('a', href=str)
                if link_tags:
                    result = []
                    for link in link_tags:
                        find_link = link['href']
                        if 'vk.com' in find_link:
                            result.append(find_link)
                    return result
                return ''
            except Exception as e:
                logger.error(f'Ошибка в файле {file_path}: {e}. Он будет пропущен.')
                return ''

    @classmethod
    def get_all_files_from_directory(self, path: str, ext: list) -> List[str]:
        '''
        Возвращает пути до всех файлов, которые содержатся в папке
        `path`: путь до необходимой папки
        `ext`: какие типы файлов необходимы (расширения файлов)
        '''
        return [join(path, f) for f in listdir(path) if isfile(join(path, f)) and splitext(join(path, f))[1] in ext]

    @classmethod
    def walk_directory(self, dir_path: str, func_handler: Callable, core_count: int = 1) -> Iterator:
        '''
        Возвращает все вложения из папки. Если указан путь до файла, операция будет выполнена только с ним
        `dir_path`: путь до папки
        `func_handler`: функция-обработчки для файлов из `dir_path`
        `core_count`: Количество используемых потоков в `ProcessPoolExecutor`
        '''
        if isfile(dir_path):
            files = [dir_path]
        else:
            files = self.get_all_files_from_directory(dir_path, ['.html'])
        with ProcessPoolExecutor(core_count) as executor:
            result = executor.map(func_handler, files)
        return result

    @classmethod
    def get_all_dirs_from_directory(self, path: str) -> List[str]:
        '''
        Возвращает путь до всех папок, находящиеся в нужной папке
        `path`: путь до нужной папки
        '''
        return [join(path, f) for f in listdir(path) if isdir(join(path, f))]

    @classmethod
    def hook_dialog_name(self, path: str, vk_encoding: str = 'cp1251') -> str | None:
        '''
        Находит имя человека, с которым велся диалог (или название диалога/беседы)
        Если имя не будет найдено, вернет `None`
        `path`: путь до папки диалога
        `vk_encoding`: Кодировка `.html` файлов VK. Обычно, `cp1251`
        '''
        with open(join(path, 'messages0.html'), 'r', encoding=vk_encoding) as f:
            html = f.read()
            soup = BeautifulSoup(html, 'html.parser')
            name = soup.find('div', class_='ui_crumb')
        return None if name is None else str(name.string)

    @classmethod
    def hook_albom_name(self, html_content: str) -> str | None:
        '''
        Находит название альбома с фото
        Если название не будет найдено, вернет `None`
        `html_content`: прочитанный `.html` файл
        '''
        soup = BeautifulSoup(html_content, 'html.parser')
        name = soup.find('div', class_='ui_crumb')
        return None if name is None else str(name.string)

    @classmethod
    def get_dialog_type(self, dialog_path: str) -> Tuple[str | int]:
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

    def __get_vk_attachments(self) -> Dict[str, dict]:
        '''
        Возвращает информацию о всех вложения в VK архиве
        `base_dir`: путь до папки VK архива
        '''
        result = {}

        all_find_links = 0

        mes_links = 0
        mes_folder = self.folder_names.get('messages', False)
        if mes_folder:
            result['messages'] = {}
            dirs = self.get_all_dirs_from_directory(join(self.archive_path, mes_folder))
            for path in dirs:
                logger.info(f'📁: {path}')
                dialog_type, dialog_id = self.get_dialog_type(path)
                dialog_full_id = f'{dialog_type}{dialog_id}'
                dialog_name = self.hook_dialog_name(path, self.vk_encoding)
                find_links = list(self.walk_directory(path, self.get_messages_attachment, self.core_count))
                logger.info(f'=> Имя диалога: {dialog_name}')
                logger.info(f'=> 🆔 диалога: {dialog_full_id}')
                all_links = {}
                for el in find_links:
                    if el:
                        for date, links in el.items():
                            if date != 'no_date':
                                date = tools.get_numberic_date(date)
                            links_storage = all_links.setdefault(date, [])
                            links_storage.extend(links)
                            mes_links += len(links)
                result['messages'][dialog_id] = {
                    'name': dialog_name,
                    'dialog_link': f'{self.vk_url}{dialog_full_id}',
                    'links': all_links
                }
            logger.info(f'🔍 Количество найденных 🔗 в {mes_folder}: {mes_links}')
            all_find_links += mes_links

        likes_photo_links = 0
        likes_photo_folder = self.folder_names.get('likes/photo', False)
        if likes_photo_folder:
            path = join(self.archive_path, likes_photo_folder)
            logger.info(f'📁: {path}')
            find_links = list(set(chain(*self.walk_directory(path, self.get_likes_attachment, self.core_count))))
            count_find_link = len(find_links)
            likes_photo_links += count_find_link
            result['likes/photo'] = {
                'links': find_links
            }
            logger.info(f'🔍 Количество найденных 🔗 в {likes_photo_folder}: {likes_photo_links}')
            all_find_links += likes_photo_links

        profile_photos_links = 0
        profile_photo_folder = self.folder_names.get('photos', False)
        if profile_photo_folder:
            result['photos'] = {}
            path = join(self.archive_path, profile_photo_folder)
            logger.info(f'📁: {path}')
            find_links = list(self.walk_directory(path, self.get_photos_attachment, self.core_count))
            for el in find_links:
                if el:
                    for albom, date_info in el.items():
                        date_info = {tools.get_numberic_date(date): links for date, links in date_info.items()}
                        date_storage = result['photos'].setdefault(albom, {})
                        date_storage.update(date_info)
                        profile_photos_links += sum(len(items) for items in date_info.values())
            logger.info(f'🔍 Количество найденных 🔗 в {profile_photo_folder}: {profile_photos_links}')
            all_find_links += profile_photos_links

        documents_links = 0
        documents_folder = self.folder_names.get('profile', False)
        if documents_folder:
            result['profile'] = {}
            dirs = self.get_all_dirs_from_directory(join(self.archive_path, documents_folder))
            path = join(self.archive_path, documents_folder, 'documents.html')
            logger.info(f'📁: {path}')
            find_links = list(self.walk_directory(path, self.get_doc_attachment, self.core_count))
            for el in find_links:
                if el:
                    for date, links in el.items():
                        if date != 'no_date':
                            date = tools.get_numberic_date(date)
                        links_storage = result['profile'].setdefault(date, [])
                        links_storage.extend(links)
                        documents_links += len(links)
            logger.info(f'🔍 Количество найденных 🔗 в {documents_folder}: {documents_links}')
            all_find_links += documents_links

        logger.info(f'🔍 Количество всех найденных 🔗: {all_find_links}')

        return result
