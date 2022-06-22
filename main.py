import json
from datetime import datetime
from os.path import join
from archive_parser import VKArchiveParser


archive_path = join('Archive', 'messages')


if __name__ == '__main__':
    start = datetime.now()
    obj = VKArchiveParser(archive_path)
    print(f'Время выполнения: {datetime.now() - start}')
    with open('links.json', 'w', encoding='utf8') as f:
        f.write(json.dumps(obj.link_info, indent=4, ensure_ascii=False))
