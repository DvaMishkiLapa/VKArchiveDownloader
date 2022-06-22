from concurrent.futures import ProcessPoolExecutor
from itertools import chain
from os import cpu_count, listdir
from os.path import isdir, isfile, join, split, splitext
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup

from logger import create_logger

logger = create_logger('logs/vk_parser.log', 'parser', 'DEBUG')


class VKArchiveParser():
    def __init__(
        self,
        archive_path: str,
        vk_url: str = 'https://vk.com/',
        vk_encoding: str = 'cp1251'
    ) -> None:
        '''
        Парсер архива VKontakte.
        `archive_path`: Путь до архива
        `vk_encoding`: Кодировка `.html` файлов VK. Обычно, `cp1251`

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
        self.cpu_count = cpu_count()
        if self.cpu_count is None:
            self.cpu_count = 1
            logger.warning('Не удалось получить число логических ядер процессора, получение ссылок будет выполнено в однопоточном режиме')
        else:
            logger.info(f'Количество потоков, используемых для получение ссылок: {self.cpu_count}')
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
                link_tags = soup.find_all('a', class_='attachment__link')
                return [tag['href'] for tag in link_tags]
            except Exception as e:
                logger.error(f'Ошибка в файле {file_path}: {e}. Он будет пропущен.')

    @classmethod
    def get_all_files_from_directory(self, path: str, ext: list) -> List[str]:
        '''
        Возвращает пути до всех файлов, которые содержатся в папке
        `path`: путь до необходимой папки
        `ext`: какие типы файлов необходимы (расширения файлов)
        '''
        return [join(path, f) for f in listdir(path) if isfile(join(path, f)) and splitext(join(path, f))[1] in ext]

    @classmethod
    def walk_dialog_directory(self, dir_path: str, cpu_count: int = 1) -> List[str]:
        '''
        Возвращает все вложения из папки диалога
        `dir_path`: путь до папки диалога
        `cpu_count`: Количество используемых потоков в `ProcessPoolExecutor`
        '''
        files = self.get_all_files_from_directory(dir_path, ['.html'])
        result = []
        with ProcessPoolExecutor(cpu_count) as executor:
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
        dirs = self.get_all_dirs_from_directory(self.archive_path)
        for path in dirs:
            logger.info(f'Обработка папки: {path}')
            dialog_type, dialog_id = self.get_dialog_type(path)
            dialog_full_id = f'{dialog_type}{dialog_id}'
            dialog_name = self.hook_dialog_name(path, self.vk_encoding)
            find_links = self.walk_dialog_directory(path, self.cpu_count)
            logger.info(f'=> Имя диалога: {dialog_name}')
            logger.info(f'=> ID диалога: {dialog_full_id}')
            logger.info(f'=> Количество найденных ссылок: {len(find_links)}')
            result[dialog_id] = {
                'name': dialog_name,
                'dialog_link': f'{self.vk_url}{dialog_full_id}',
                'links': find_links
            }
        return result
