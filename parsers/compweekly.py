from bs4 import BeautifulSoup
import requests
from typing import Optional, Dict, Callable
import logging

def parse_article_with_metadata(html_content):
    """
    Парсит статью с метаданными
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    result = {
        'title': None,
        'content': '',
        'chapters': []
    }

    # Пытаемся найти заголовок (может быть в разных местах)
    title = (soup.find('h1') or
             soup.find('title') or
             soup.find('meta', property='og:title'))

    if title:
        if hasattr(title, 'text'):
            result['title'] = title.text.strip()
        elif hasattr(title, 'get'):
            result['title'] = title.get('content', '').strip()

    # Парсим основной контент
    content_body = soup.find('section', id='content-body')
    if content_body:
        paragraphs = []
        for p in content_body.find_all('p'):
            text = p.get_text(strip=True)
            if text and not text.startswith('@'):
                paragraphs.append(text)
        result['content'] = ' '.join(paragraphs)

    # Парсим главы/разделы
    chapters = soup.find_all('section', class_='section main-article-chapter')
    for chapter in chapters:
        chapter_title = chapter.get('data-menu-title', '').strip()
        if chapter_title:
            result['chapters'].append(chapter_title)

    return result


# Пример использования
def parse_article_from_url(url):
    """
    Полная функция для парсинга статьи по URL
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        # Парсим статью
        article_data = parse_article_with_metadata(response.text)

        return article_data

    except Exception as e:
        print(f"Ошибка при парсинге: {e}")
        return None





# Пример использования
def fetch_text_computerweekly(url) -> Optional[str]:


    result = parse_article_from_url(url)
    return result['content']
