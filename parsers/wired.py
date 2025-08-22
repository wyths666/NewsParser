import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime


def parse_wired_article(url):
    """
    Парсит статью с Wired.com
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }

        print(f"Загружаем статью: {url}")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Извлекаем основные данные
        article_data = {
            'title': extract_wired_title(soup),
            'content': extract_wired_content(soup),
            'author': extract_wired_author(soup),
            'publish_date': extract_wired_publish_date(soup),
            'summary': extract_wired_summary(soup),
            'tags': extract_wired_tags(soup),
            'images': extract_wired_images(soup),
            'url': url
        }

        return article_data

    except Exception as e:
        print(f"Ошибка при парсинге статьи: {e}")
        return None


def extract_wired_title(soup):
    """Извлекает заголовок статьи Wired"""
    selectors = [
        'h1[data-testid="ContentHeaderHed"]',
        'h1.headline',
        'h1.article-title',
        'h1',
        'title'
    ]

    for selector in selectors:
        element = soup.select_one(selector)
        if element and element.text.strip():
            return clean_text(element.text.strip())

    return "Заголовок не найден"


def extract_wired_content(soup):
    """Извлекает основной контент статьи Wired"""
    content_selectors = [
        'div[data-testid="ContentHeaderAccreditation"] + div',  # Контент после заголовка
        'article div.body__inner-container',
        '.article-body',
        '.post-content',
        '[data-attribute-verso-pattern="article-body"]'
    ]

    content_text = []

    for selector in content_selectors:
        content_element = soup.select_one(selector)
        if content_element:
            # Извлекаем текст из всех параграфов
            paragraphs = content_element.find_all(['p', 'h2', 'h3'], recursive=True)

            for element in paragraphs:
                # Пропускаем рекламу и ненужные элементы
                if element.find_parents(['aside', 'div.ad-wrapper', 'figure']):
                    continue

                text = clean_text(element.get_text())
                if text and len(text) > 10:  # Игнорируем короткие тексты
                    content_text.append(text)

            if content_text:
                break

    return '\n\n'.join(content_text) if content_text else "Контент не найден"


def extract_wired_author(soup):
    """Извлекает автора статьи Wired"""
    author_selectors = [
        'a[data-testid="AuthorBioLink"]',
        '.byline-component__content a',
        '.author-name',
        'meta[name="author"]',
        '[rel="author"]'
    ]

    for selector in author_selectors:
        element = soup.select_one(selector)
        if element:
            if element.name == 'meta':
                return clean_text(element.get('content', ''))
            return clean_text(element.text)

    return "Автор не указан"


def extract_wired_publish_date(soup):
    """Извлекает дату публикации Wired"""
    date_selectors = [
        'time[datetime]',
        '[data-testid="ContentHeaderPublishDate"]',
        '.publish-date',
        'meta[property="article:published_time"]'
    ]

    for selector in date_selectors:
        element = soup.select_one(selector)
        if element:
            if element.name == 'meta':
                date_str = element.get('content', '')
                return format_date(date_str)
            datetime_attr = element.get('datetime', '')
            if datetime_attr:
                return format_date(datetime_attr)
            return clean_text(element.text)

    return "Дата не указана"


def extract_wired_summary(soup):
    """Извлекает краткое описание/саммари статьи"""
    summary_selectors = [
        '[data-testid="ContentHeaderDek"]',
        '.article-summary',
        '.dek',
        'meta[property="og:description"]'
    ]

    for selector in summary_selectors:
        element = soup.select_one(selector)
        if element:
            if element.name == 'meta':
                return clean_text(element.get('content', ''))
            return clean_text(element.text)

    return ""


def extract_wired_tags(soup):
    """Извлекает теги/категории Wired"""
    tags = []
    tag_selectors = [
        '[data-testid="TopicTags"] a',
        '.tags a',
        '.categories a',
        '.topic-list a'
    ]

    for selector in tag_selectors:
        elements = soup.select(selector)
        for element in elements:
            tag_text = clean_text(element.text)
            if tag_text:
                tags.append(tag_text)

    return tags


def extract_wired_images(soup):
    """Извлекает изображения из статьи Wired"""
    images = []
    img_selectors = [
        'article img',
        '.body__inner-container img',
        '[data-testid*="Image"] img'
    ]

    for selector in img_selectors:
        img_elements = soup.select(selector)
        for img in img_elements:
            src = img.get('src') or img.get('data-src')
            if src and not src.startswith('data:') and 'wired.com' in src:
                images.append({
                    'src': src,
                    'alt': img.get('alt', ''),
                    'caption': find_image_caption(img)
                })

    return images


def find_image_caption(img_element):
    """Находит подпись к изображению"""
    # Ищем в родительском элементе или соседях
    parent = img_element.parent
    if parent:
        caption = parent.find(['figcaption', 'p', 'div'], class_=lambda x: x and any(
            word in str(x).lower() for word in ['caption', 'credit', 'description']))
        if caption:
            return clean_text(caption.text)

    return ""


def clean_text(text):
    """Очищает текст от лишних пробелов и символов"""
    if not text:
        return ""

    # Удаляем лишние пробелы и переносы
    text = re.sub(r'\s+', ' ', text.strip())
    # Удаляем непечатаемые символы
    text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)

    return text


def format_date(date_str):
    """Форматирует дату в читаемый вид"""
    try:
        # Пробуем разные форматы дат
        for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d']:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
        return date_str
    except:
        return date_str


# Пример использования
def fetch_text_wired(url):

    article = parse_wired_article(url)

    return article['content']