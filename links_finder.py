from concurrent.futures import ProcessPoolExecutor
from itertools import chain
from os import cpu_count, listdir
from os.path import isdir, isfile, join, split, splitext, basename
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup

from logger import create_logger

logger = create_logger('logs/vk_parser.log', 'parser', 'DEBUG')


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
    def get_attachment(self, file_path: str, vk_encoding: str = 'cp1251') -> List[str] | None:
        '''
        Возвращает все ссылки на вложения из `html` файла
        `file_path`: путь до файла для чтения
        `vk_encoding`: Кодировка `.html` файлов VK. Обычно, `cp1251`
        '''
        with open(file_path, encoding=vk_encoding) as f:
            try:
                soup = BeautifulSoup(f.read(), 'html.parser')

                # for link in messages
                link_tags = soup.find_all('a', class_='attachment__link', href=str)
                if link_tags:
                    return [tag['href'] for tag in link_tags]

                # for link profile photos
                link_tags = soup.find_all('img', src=str)
                if link_tags:
                    result = []
                    for link in link_tags:
                        find_link = link['src']
                        if 'http' in find_link:
                            result.append(find_link)
                    return result

                # for link in likes -> photo
                link_tags = soup.find_all('a', href=str)
                if link_tags:
                    result = []
                    for link in link_tags:
                        find_link = link['href']
                        if 'vk.com/photo' in find_link:
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
    def walk_dialog_directory(self, dir_path: str, core_count: int = 1) -> List[str]:
        '''
        Возвращает все вложения из папки диалога
        `dir_path`: путь до папки диалога
        `core_count`: Количество используемых потоков в `ProcessPoolExecutor`
        '''
        files = self.get_all_files_from_directory(dir_path, ['.html'])
        result = []
        with ProcessPoolExecutor(core_count) as executor:
            result = list(chain(*executor.map(self.get_attachment, files)))
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
        path`: путь до папки диалога
        `vk_encoding`: Кодировка `.html` файлов VK. Обычно, `cp1251`
        '''
        with open(join(path, 'messages0.html'), 'r', encoding=vk_encoding) as f:
            html = f.read()
            soup = BeautifulSoup(html, 'html.parser')
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
                find_links = self.walk_dialog_directory(path, self.core_count)
                count_find_link = len(find_links)
                mes_links += count_find_link
                logger.info(f'=> Имя диалога: {dialog_name}')
                logger.info(f'=> 🆔 диалога: {dialog_full_id}')
                logger.info(f'=> Количество найденных 🔗: {count_find_link}')
                result['messages'][dialog_id] = {
                    'name': dialog_name,
                    'dialog_link': f'{self.vk_url}{dialog_full_id}',
                    'links': find_links
                }
        logger.info(f'🔍 Количество найденных 🔗 в {mes_folder}: {mes_links}')
        all_find_links += mes_links

        likes_photo_links = 0
        likes_photo_folder = self.folder_names.get('likes_photo', False)
        if likes_photo_folder:
            path = join(self.archive_path, likes_photo_folder)
            logger.info(f'📁: {path}')
            find_links = self.walk_dialog_directory(path, self.core_count)
            count_find_link = len(find_links)
            likes_photo_links += count_find_link
            result['likes_photo'] = {
                'links': find_links
            }
        logger.info(f'🔍 Количество найденных 🔗 в {likes_photo_folder}: {likes_photo_links}')
        all_find_links += likes_photo_links

        profile_photos_links = 0
        profile_photo_folder = self.folder_names.get('photos', False)
        if profile_photo_folder:
            result['photos'] = {}
            dirs = self.get_all_dirs_from_directory(join(self.archive_path, profile_photo_folder))
            for path in dirs:
                logger.info(f'📁: {path}')
                find_links = self.walk_dialog_directory(path, self.core_count)
                count_find_link = len(find_links)
                profile_photos_links += count_find_link
                logger.info(f'=> Количество найденных 🔗: {count_find_link}')
                result['photos'][basename(path)] = {
                    'links': find_links
                }
        logger.info(f'🔍 Количество найденных 🔗 в {profile_photo_folder}: {profile_photos_links}')
        all_find_links += profile_photos_links

        logger.info(f'🔍 Количество всех найденных 🔗: {all_find_links}')

        return result
