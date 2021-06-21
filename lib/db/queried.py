from lib.utils.datetime_helpers import day_wrapping_datetimes
from datetime import datetime


QUERIES = {
    'pinkygloves': {
        'query': '#pinkygloves OR #pinkygate',
        'data_start_date': day_wrapping_datetimes(datetime.strptime("2021-04-09", '%Y-%m-%d'))[0],
        'data_end_date': day_wrapping_datetimes(datetime.strptime("2021-05-29", '%Y-%m-%d'))[1],
        'true_start_date': day_wrapping_datetimes(datetime.strptime("2021-04-13", '%Y-%m-%d'))[0],
        'true_end_date': day_wrapping_datetimes(datetime.strptime("2021-04-23", '%Y-%m-%d'))[1]
        # eng waves happens between 17th and 23d
    },
    'amthor': {
        'query': '#amthor OR #amthorgate OR #AmthorRuecktritt',
        'data_start_date': day_wrapping_datetimes(datetime.strptime("2020-06-5", '%Y-%m-%d'))[0],
        'data_end_date': day_wrapping_datetimes(datetime.strptime("2020-06-22", '%Y-%m-%d'))[1],
        'true_start_date': day_wrapping_datetimes(datetime.strptime("2020-06-12", '%Y-%m-%d'))[0],
        'true_end_date': None
    },
    'spahnruecktritt': {
        'query': '#spahnruecktritt OR #spahnruecktrittjetzt',
        'data_start_date': day_wrapping_datetimes(datetime.strptime("2021-05-28", '%Y-%m-%d'))[0],
        'data_end_date': day_wrapping_datetimes(datetime.strptime("2021-06-08", '%Y-%m-%d'))[1],
        'true_start_date': day_wrapping_datetimes(datetime.strptime("2021-06-04", '%Y-%m-%d'))[0],
        'true_end_date': None
    },
    'masken_csu': {
        'query': '#Nuesslein OR #Maskendeals OR #Maskenaffäre OR #Aserbaidschan OR #schwarzerFilz',
        'data_start_date': day_wrapping_datetimes(datetime.strptime("2021-02-18", '%Y-%m-%d'))[0],
        'data_end_date': day_wrapping_datetimes(datetime.strptime("2021-03-06", '%Y-%m-%d'))[1],
        'true_start_date':day_wrapping_datetimes(datetime.strptime("2021-02-24", '%Y-%m-%d'))[0],
        'true_end_date': None
    },
    'lehmann': {
        'query': 'Aogo OR Lehmann',
        'data_start_date': day_wrapping_datetimes(datetime.strptime("2021-04-28", '%Y-%m-%d'))[0],
        'data_end_date': day_wrapping_datetimes(datetime.strptime("2021-05-15", '%Y-%m-%d'))[1],
        'true_start_date': day_wrapping_datetimes(datetime.strptime("2021-05-05", '%Y-%m-%d'))[0],
        'true_end_date': day_wrapping_datetimes(datetime.strptime("2021-05-09", '%Y-%m-%d'))[1],
    }
}

for key in QUERIES:
    if 'true_start_date' not in QUERIES[key] or not QUERIES[key]['true_start_date']:
        QUERIES[key]['true_start_date'] = QUERIES[key]['data_start_date']
    if 'true_end_date' not in QUERIES[key] or not QUERIES[key]['true_end_date']:
        QUERIES[key]['true_end_date'] = QUERIES[key]['data_end_date']


if __name__ == "__main__":
    print(QUERIES)