import json
from datetime import datetime
from os.path import join

from archive_parser import VKArchiveParser
from logger import create_logger

logger = create_logger('logs/vk_parser.log', 'main', 'DEBUG')
archive_path = join('Archive', 'messages')


if __name__ == '__main__':
    logger.info('[==> Начат процесс получения данных из архива VK... <==]')
    start = datetime.now()
    obj = VKArchiveParser(archive_path)
    logger.info(f'Время создания файла JSON с информацией о ссылках: {datetime.now() - start}')
    with open('links.json', 'w', encoding='utf8') as f:
        f.write(json.dumps(obj.link_info, indent=4, ensure_ascii=False))
