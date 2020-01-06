from lxml import html
from pprint import pprint  # noqa

API_URL = 'https://www.eccourts.org/api/get_posts/'


def clean_text(text):
    try:
        return html.fromstring(text).text
    except Exception:
        return text


def emit_attachment(context, post, attachment):
    meta = {
        'title': clean_text(attachment.get('title')),
        'summary': clean_text(attachment.get('description')),
        'languages': ['en'],
        'author': post.get('author', {}).get('name'),
        'published_at': post.get('date'),
        'modified_at': post.get('modified'),
        'mime_type': attachment.get('mime_type'),
        'foreign_id': attachment.get('url'),
        'url': attachment.get('url'),
        'keywords': []
    }
    for cat in post.get('categories', []):
        meta['keywords'].append(cat.get('title'))

    context.emit(data=meta)


def posts(context, data):
    page = data.get('page', 0)
    result = context.http.get(API_URL, params={'page': page})
    if not result.ok:
        context.emit_warning("Response failure: %r" % result)
        return

    for post in result.json.get('posts'):
        for attachment in post.get('attachments'):
            if 'image/' in attachment.get('mime_type'):
                continue
            emit_attachment(context, post, attachment)

    pages = result.json.get('pages')
    if pages > page:
        context.recurse(data={'page': page + 1})
