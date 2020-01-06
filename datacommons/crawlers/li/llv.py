CATEGORIES = ['HREIN', 'STEUER', 'EDKURLG', 'GLB', 'HRKU', 'KONZ',
              'GBBER', 'VAUFT', 'ZWLI', 'ZUST']


def latest(context, data):
    latest_id = 0
    for category in CATEGORIES:
        # Check the relevant RSS feeds to see which one was updated most
        # recently, and get the latest notice id from the top
        feed = context.params.get('rss_url') % category
        res = context.http.get(feed)
        rss = res.xml
        link = rss.findtext('.//item/link')
        if link is None or link == '':
            context.log.info("Nothing in RSS /%s found." % category)
        else:
            top_id = int(link.rsplit('/')[-1]) + 50
            if top_id > latest_id:
                latest_id = top_id
    context.emit(data={'number': latest_id})


def crawl(context, data):
    url = data.get('url')
    res = context.http.get(url)
    doc = res.html

    category = doc.find('.//div[@class="details"]//a')
    if category is None:
        return
    if category.get('href').split('/')[-1] not in CATEGORIES:
        return

    foreign_id = url.split("/display/")[-1]
    title = doc.findtext('.//div[@class="body"]//h3')
    data = {
        'title': title,
        'countries': ['li'],
        'languages': ['de'],
        'extension': 'pdf',
        'mime_type': 'application/pdf',
        'foreign_id': foreign_id,
        'source_url': url,
        'url': context.params.get('print_url'),
    }
    context.emit(data=data)
