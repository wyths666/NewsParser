import requests
from bs4 import BeautifulSoup
import re


def parse_engadget_article(url):
    """
    Парсит статью с Engadget-подобного сайта
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }

        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Извлекаем основные данные
        article_data = {
            'title': extract_title(soup),
            'content': extract_content(soup),
            'author': extract_author(soup),
            'publish_date': extract_publish_date(soup),
            'tags': extract_tags(soup),
            'images': extract_images(soup),
            'url': url
        }

        return article_data

    except Exception as e:
        print(f"Ошибка при парсинге статьи: {e}")
        return None


def extract_title(soup):
    """Извлекает заголовок статьи"""
    # Пробуем разные селекторы для заголовка
    selectors = [
        'h1',
        '[data-testid="article-title"]',
        'header h1',
        '.article-title',
        'title'
    ]

    for selector in selectors:
        element = soup.select_one(selector)
        if element and element.text.strip():
            return element.text.strip()

    return "Заголовок не найден"


def extract_content(soup):
    """Извлекает основной контент статьи"""
    # Ищем основной контент по различным селекторам
    content_selectors = [
        '[data-article-body="true"]',
        '.article-content',
        '.post-content',
        '.entry-content',
        '[class*="body"]',
        '[class*="content"]',
        'article'
    ]

    content_text = []

    for selector in content_selectors:
        content_element = soup.select_one(selector)
        if content_element:
            # Извлекаем текст из всех параграфов
            paragraphs = content_element.find_all(['p', 'div'], class_=lambda x: x != 'ad-wrapper')

            for p in paragraphs:
                text = clean_text(p.get_text())
                if text and len(text) > 20:  # Игнорируем короткие тексты
                    content_text.append(text)

            if content_text:
                break

    return '\n\n'.join(content_text) if content_text else "Контент не найден"


def extract_author(soup):
    """Извлекает автора статьи"""
    author_selectors = [
        '[data-testid="author-name"]',
        '.author-name',
        '.byline a',
        '[rel="author"]',
        'meta[name="author"]'
    ]

    for selector in author_selectors:
        element = soup.select_one(selector)
        if element:
            if element.name == 'meta':
                return element.get('content', '').strip()
            return element.text.strip()

    return "Автор не указан"


def extract_publish_date(soup):
    """Извлекает дату публикации"""
    date_selectors = [
        'time[datetime]',
        '.publish-date',
        '.date-published',
        'meta[property="article:published_time"]',
        '[data-testid="published-date"]'
    ]

    for selector in date_selectors:
        element = soup.select_one(selector)
        if element:
            if element.name == 'meta':
                return element.get('content', '').strip()
            return element.get('datetime', element.text).strip()

    return "Дата не указана"


def extract_tags(soup):
    """Извлекает теги/категории"""
    tags = []
    tag_selectors = [
        '.tags a',
        '.categories a',
        '[rel="tag"]',
        '.topic-tags a'
    ]

    for selector in tag_selectors:
        elements = soup.select(selector)
        for element in elements:
            tag_text = clean_text(element.text)
            if tag_text:
                tags.append(tag_text)

    return tags


def extract_images(soup):
    """Извлекает изображения из статьи"""
    images = []
    img_selectors = [
        'article img',
        '.article-content img',
        '.post-content img'
    ]

    for selector in img_selectors:
        img_elements = soup.select(selector)
        for img in img_elements:
            src = img.get('src') or img.get('data-src')
            if src and not src.startswith('data:'):
                images.append({
                    'src': src,
                    'alt': img.get('alt', ''),
                    'caption': find_image_caption(img)
                })

    return images


def find_image_caption(img_element):
    """Находит подпись к изображению"""
    # Ищем в соседних элементах
    parent = img_element.parent
    if parent:
        caption = parent.find(['figcaption', 'p', 'div'], class_=lambda x: x and 'caption' in x.lower())
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


# Пример использования
def fetch_text_engadget(url):
    article = parse_engadget_article(url)
    return article['content']