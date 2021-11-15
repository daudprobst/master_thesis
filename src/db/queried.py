from datetime import datetime

from src.utils.datetime_helpers import day_wrapping_datetimes

QUERIES = {
    "pinkygloves": {
        "query": "#pinkygloves OR #pinkygate",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-04-09", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-05-29", "%Y-%m-%d")
        )[1],
        "true_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-04-13", "%Y-%m-%d")
        )[0],
        "true_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-04-15", "%Y-%m-%d")
        )[1]
        # eng waves happens between 17th and 23d
    },
    "amthor": {
        "query": "#amthor OR #amthorgate OR #AmthorRuecktritt",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2020-06-5", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2020-06-30", "%Y-%m-%d")
        )[1],
        "true_start_date": day_wrapping_datetimes(
            datetime.strptime("2020-06-12", "%Y-%m-%d")
        )[0],
        "true_end_date": None,
    },
    "spahnruecktritt": {
        "query": "#spahnruecktritt OR #spahnruecktrittjetzt",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-05-28", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-06-20", "%Y-%m-%d")
        )[1],
        "true_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-06-04", "%Y-%m-%d")
        )[0],
        "true_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-06-06", "%Y-%m-%d")
        )[1],
    },
    "masken_csu": {
        "query": "#Nuesslein OR #Maskendeals OR #Maskenaffäre OR #Aserbaidschan OR #schwarzerFilz",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-02-18", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-03-06", "%Y-%m-%d")
        )[1],
        "true_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-02-24", "%Y-%m-%d")
        )[0],
        "true_end_date": None,
    },
    "lehmann": {
        "query": "Aogo OR Lehmann",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-04-28", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-05-15", "%Y-%m-%d")
        )[1],
        "true_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-05-05", "%Y-%m-%d")
        )[0],
        "true_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-05-08", "%Y-%m-%d")
        )[1],
    },
    "studierenWieBaerbock": {
        "query": "#studierenwieBaerbock OR #Bundescancelerin OR #baerbockfail",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-05-07", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-05-24", "%Y-%m-%d")
        )[1],
        "true_start_date": None,
        "true_end_date": None,
    },
    "omasau": {
        "query": "#Omasau OR #OmaGate OR #UmweltsauOma OR #OmaUmweltsau OR conversation_id:1211673284321918977 "
        "OR conversation_id:1210935523545620480",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2019-12-21", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2020-01-07", "%Y-%m-%d")
        )[1],
        "true_start_date": day_wrapping_datetimes(
            datetime.strptime("2019-12-28", "%Y-%m-%d")
        )[0],
        "true_end_date": day_wrapping_datetimes(
            datetime.strptime("2020-01-01", "%Y-%m-%d")
        )[1],
    },
    "helmeLookLikeShit": {
        "query": "#HelmeRettenLeben OR #lookslikeshit OR #saveslifes OR conversation_id:1108842805089177615",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2019-03-14", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2019-03-31", "%Y-%m-%d")
        )[1],
        "true_start_date": None,
        "true_end_date": None,
    },
    "studentinnenfutter": {
        "query": "#Studentinnenfutter OR Studierendenfutter OR conversation_id:1413204084694437888",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-07-01", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-07-18", "%Y-%m-%d")
        )[1],
        "true_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-07-09", "%Y-%m-%d")
        )[0],
        "true_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-07-09", "%Y-%m-%d")
        )[1],
    },
    "dbUnternehmerischeEntscheidung": {
        "query": "conversation_id:1359969795068727296 OR conversation_id:1349076313315942404 OR "
        "conversation_id:1359926029909827585",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-02-04", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-02-21", "%Y-%m-%d")
        )[1],
        "true_start_date": None,
        "true_end_date": None,
    },
    "thunbergsVsDB": {
        "query": "conversation_id:1206182673888219136",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2019-12-08", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2019-12-25", "%Y-%m-%d")
        )[1],
        "true_start_date": day_wrapping_datetimes(
            datetime.strptime("2019-12-15", "%Y-%m-%d")
        )[0],
        "true_end_date": day_wrapping_datetimes(
            datetime.strptime("2019-12-16", "%Y-%m-%d")
        )[1],
    },
    "kloecknerNestle": {
        "query": "(#Klöckner #Nestle) OR conversation_id:1135553266476040192",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2019-05-27", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2019-06-13", "%Y-%m-%d")
        )[1],
        "true_start_date": day_wrapping_datetimes(
            datetime.strptime("2019-06-05", "%Y-%m-%d")
        )[0],
        "true_end_date": day_wrapping_datetimes(
            datetime.strptime("2019-06-07", "%Y-%m-%d")
        )[1],
    },
    "uefaUngarn": {
        "query": "#MuenchenMachEsTrotzdem OR (#Regenbogen #UEFA) OR (#Regenbogenfarben #UEFA) OR (#AllianzArena #UEFA) "
        "OR (#UEFA #GERHUN) OR (#Regenbogen #EURO2020) OR (#Regenbogenfarben #EURO2020)",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-06-14", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-07-01", "%Y-%m-%d")
        )[1],
        "true_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-06-20", "%Y-%m-%d")
        )[0],
        "true_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-06-23", "%Y-%m-%d")
        )[1],
    },
    "laschetLacht": {
        "query": "#Laschetlacht OR #LaschetRuecktritt OR #LautGegenLaschet OR conversation_id:1416449185172369411",
        "data_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-07-10", "%Y-%m-%d")
        )[0],
        "data_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-07-27", "%Y-%m-%d")
        )[1],
        "true_start_date": day_wrapping_datetimes(
            datetime.strptime("2021-07-17", "%Y-%m-%d")
        )[0],
        "true_end_date": day_wrapping_datetimes(
            datetime.strptime("2021-07-19", "%Y-%m-%d")
        )[1],
    },
}

for key in QUERIES:
    if "true_start_date" not in QUERIES[key] or not QUERIES[key]["true_start_date"]:
        QUERIES[key]["true_start_date"] = QUERIES[key]["data_start_date"]
    if "true_end_date" not in QUERIES[key] or not QUERIES[key]["true_end_date"]:
        QUERIES[key]["true_end_date"] = QUERIES[key]["data_end_date"]
