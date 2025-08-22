import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import json


def parse_cnet_article(url):
    """
    Парсит статью с CNET.com
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': 'https://www.cnet.com/'
        }


        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Извлекаем основные данные
        article_data = {
            'title': extract_cnet_title(soup),
            'content': extract_cnet_content(soup),
            'author': extract_cnet_author(soup),
            'publish_date': extract_cnet_publish_date(soup),
            'update_date': extract_cnet_update_date(soup),
            'summary': extract_cnet_summary(soup),
            'tags': extract_cnet_tags(soup),
            'category': extract_cnet_category(soup),
            'images': extract_cnet_images(soup),
            'rating': extract_cnet_rating(soup),
            'url': url
        }

        return article_data

    except Exception as e:
        print(f"Ошибка при парсинге статьи: {e}")
        return None


def extract_cnet_title(soup):
    """Извлекает заголовок статьи CNET"""
    selectors = [
        'h1[data-testid="title"]',
        'h1.articleHead',
        'h1.headline',
        'h1',
        'title'
    ]

    for selector in selectors:
        element = soup.select_one(selector)
        if element and element.text.strip():
            return clean_text(element.text.strip())

    return "Заголовок не найден"


def extract_cnet_content(soup):
    """Извлекает основной контент статьи CNET"""
    content_selectors = [
        'div[data-testid="body"]',
        'div.article-body',
        '.c-pageArticle_content',
        '.c-shortcodeArticle-content',
        '.post-content'
    ]

    content_text = []

    for selector in content_selectors:
        content_element = soup.select_one(selector)
        if content_element:
            # Удаляем рекламные блоки и ненужные элементы
            for unwanted in content_element.select('.ad-unit, .inline-ad, .c-marketplace, .c-productComparison'):
                unwanted.decompose()

            # Извлекаем текст из всех параграфов
            paragraphs = content_element.find_all(['p', 'h2', 'h3', 'h4'], recursive=True)

            for element in paragraphs:
                # Пропускаем рекламу и ненужные элементы
                if element.find_parents(['aside', 'div.ad-wrapper', 'figure', '.c-productComparison']):
                    continue

                text = clean_text(element.get_text())
                if text and len(text) > 15 and not text.startswith(
                        'See at'):  # Игнорируем короткие тексты и рекламные ссылки
                    content_text.append(text)

            if content_text:
                break

    return '\n\n'.join(content_text) if content_text else "Контент не найден"


def extract_cnet_author(soup):
    """Извлекает автора статьи CNET"""
    author_selectors = [
        'a[data-testid="authorLink"]',
        '.c-byline__author a',
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


def extract_cnet_publish_date(soup):
    """Извлекает дату публикации CNET"""
    date_selectors = [
        'time[datetime]',
        '[data-testid="publishDate"]',
        '.c-byline__published',
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


def extract_cnet_update_date(soup):
    """Извлекает дату обновления статьи CNET"""
    update_selectors = [
        '[data-testid="updateDate"]',
        '.c-byline__updated',
        '.update-date'
    ]

    for selector in update_selectors:
        element = soup.select_one(selector)
        if element:
            datetime_attr = element.get('datetime', '')
            if datetime_attr:
                return format_date(datetime_attr)
            return clean_text(element.text)

    return ""


def extract_cnet_summary(soup):
    """Извлекает краткое описание/саммари статьи CNET"""
    summary_selectors = [
        '[data-testid="dek"]',
        '.c-pageArticle_dek',
        '.article-dek',
        'meta[property="og:description"]',
        'meta[name="description"]'
    ]

    for selector in summary_selectors:
        element = soup.select_one(selector)
        if element:
            if element.name == 'meta':
                return clean_text(element.get('content', ''))
            return clean_text(element.text)

    return ""


def extract_cnet_tags(soup):
    """Извлекает теги/категории CNET"""
    tags = []
    tag_selectors = [
        '[data-testid="tagList"] a',
        '.c-tagList a',
        '.tags a',
        '.categories a'
    ]

    for selector in tag_selectors:
        elements = soup.select(selector)
        for element in elements:
            tag_text = clean_text(element.text)
            if tag_text:
                tags.append(tag_text)

    return tags


def extract_cnet_category(soup):
    """Извлекает категорию статьи CNET"""
    category_selectors = [
        '[data-testid="breadcrumb"] a:last-child',
        '.c-breadcrumbs a:last-child',
        '.breadcrumb a:last-child'
    ]

    for selector in category_selectors:
        element = soup.select_one(selector)
        if element:
            return clean_text(element.text)

    return ""


def extract_cnet_rating(soup):
    """Извлекает рейтинг (если это обзор)"""
    rating_selectors = [
        '[data-testid="rating"]',
        '.c-reviewScore',
        '.rating'
    ]

    for selector in rating_selectors:
        element = soup.select_one(selector)
        if element:
            return clean_text(element.text)

    return ""


def extract_cnet_images(soup):
    """Извлекает изображения из статьи CNET"""
    images = []
    img_selectors = [
        'article img',
        '.c-pageArticle_content img',
        '[data-testid*="image"] img',
        '.c-figure img'
    ]

    for selector in img_selectors:
        img_elements = soup.select(selector)
        for img in img_elements:
            src = img.get('src') or img.get('data-src')
            if src and not src.startswith('data:') and any(domain in src for domain in ['cnet.com', 'cnbcfm.com']):
                images.append({
                    'src': src,
                    'alt': img.get('alt', ''),
                    'caption': find_cnet_image_caption(img)
                })

    return images


def find_cnet_image_caption(img_element):
    """Находит подпись к изображению в CNET"""
    # Ищем в родительском элементе figure
    parent = img_element.parent
    if parent and parent.name == 'figure':
        caption = parent.find('figcaption')
        if caption:
            return clean_text(caption.text)

    # Ищем в соседних элементах
    next_sibling = img_element.find_next_sibling('p')
    if next_sibling and any(word in next_sibling.get('class', []) for word in ['caption', 'credit']):
        return clean_text(next_sibling.text)

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
def fetch_text_cnet(url):

    article = parse_cnet_article(url)

    return article['content']


