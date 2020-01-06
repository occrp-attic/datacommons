from datetime import datetime


def sequence(context, data):
    this_year = int(datetime.now().strftime("%Y"))
    years = range(this_year, this_year-6, -1)
    issues = range(1, 367)
    for year in years:
        for issue in issues:
            data = {
                'year': year,
                'issue': issue
            }
            context.emit(data=data)
