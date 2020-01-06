from urllib.parse import urljoin
import json

# https://www.fara.gov/search.html
INIT_URL = 'https://efile.fara.gov/pls/apex/f?p=135:10:6633045485587::NO::P10_DOCTYPE:ALL'  # noqa
FLOW_URL = 'https://efile.fara.gov/ords/wwv_flow.accept'


def init(context, data):
    form_page = context.http.get(INIT_URL)
    doc = form_page.html

    args = {
        "p_json": json.dumps({
                "salt": doc.xpath('//*[@id="pSalt"]')[0].get('value'),
                "pageItems": {
                    "itemsToSubmit": [
                        {
                            "n": "P10_DOCTYPE",
                            "v": "ALL"
                        },
                        {
                            "n": "P10_STATUS",
                            "v": "ALL"
                        },
                        {
                            "n": "P10_REG_NUMBER",
                            "v": ""
                        },
                        {
                            "n": "P10_REG_NAME",
                            "v": ""
                        },
                        {
                            "n": "P10_REGNAME_FUZZY",
                            "v": "E"
                        },
                        {
                            "n": "P10_STAMP1",
                            "v": ""
                        },
                        {
                            "n": "P10_STAMP2",
                            "v": ""
                        },
                        {
                            "n": "P10_SHOW_SEARCH",
                            "v": "",
                            "ck": doc.xpath('//*[@data-for="P10_SHOW_SEARCH"]')[0].get('value'),  # noqa
                        },
                        {
                            "n": "P10_REGDATE_LABEL",
                            "v": "Registration Date"
                        }
                    ],
                    "protected": doc.xpath('//*[@id="pPageItemsProtected"]')[0].get('value'),  # noqa
                    "rowVersion": ""
                }
            }),
        "p_flow_id": doc.xpath('//*[@id="pFlowId"]')[0].get('value'),
        "p_flow_step_id": doc.xpath('//*[@id="pFlowStepId"]')[0].get('value'),
        "p_instance": doc.xpath('//*[@id="pInstance"]')[0].get('value'),
        "p_page_submission_id": doc.xpath('//*[@id="pPageSubmissionId"]')[0].get('value'),  # noqa
        "p_request": 'SEARCH',
        "p_reload_on_submit": 'A'
    }

    result = context.http.post(FLOW_URL, data=args)
    data.update(result.serialize())
    context.emit(data=data)


def parse(context, data):
    with context.http.rehash(data) as result:
        table = result.html.find('.//table[@id="report_R249155080118566622"]')
        for link in table.findall('.//a'):
            url = urljoin(INIT_URL, link.get('href'))
            if link.text is not None and 'Next' in link.text:
                context.emit(rule='page', data={'url': url})
            if '/docs/' in url:
                context.emit(rule='fetch', data={'url': url})
