from datetime import datetime
import pytz
from src.utils.datetime_helpers import day_wrapping_datetimes

DATE_FORMAT = "%Y-%m-%d"
GER_TZ = pytz.timezone("Europe/Berlin")


QUERIES = {
    "pinkygloves": {
        "query": "#pinkygloves OR #pinkygate",
        "data_start_date": "2021-04-09",
        "data_end_date": "2021-05-29",
        "true_start_date": "2021-04-13",
        "true_end_date": "2021-04-15"
        # eng waves happens between 17th and 23d
    },
    "amthor": {
        "query": "#amthor OR #amthorgate OR #AmthorRuecktritt",
        "data_start_date": "2020-06-5",
        "data_end_date": "2020-06-30",
        "true_start_date": "2020-06-12",
        "true_end_date": "2020-06-21",
    },
    "spahnruecktritt": {
        "query": "#spahnruecktritt OR #spahnruecktrittjetzt",
        "data_start_date": "2021-05-28",
        "data_end_date": "2021-06-20",
        "true_start_date": "2021-06-04",
        "true_end_date": "2021-06-06",
    },
    "masken_csu": {
        "query": "#Nuesslein OR #Maskendeals OR #Maskenaffäre OR #Aserbaidschan OR #schwarzerFilz",
        "disabled": True,  # disabled as tweets returned were not matching the firestorm well
        "data_start_date": "2021-02-18",
        "data_end_date": "2021-03-06",
        "true_start_date": "2021-02-24",
        "true_end_date": None,
    },
    "lehmann": {
        "query": "Aogo OR Lehmann",
        "data_start_date": "2021-04-28",
        "data_end_date": "2021-05-15",
        "true_start_date": "2021-05-05",
        "true_end_date": "2021-05-06",
    },
    "studierenWieBaerbock": {
        "query": "#studierenwieBaerbock OR #Bundescancelerin OR #baerbockfail",
        "disabled": True,  # disabled for now as there are two spikes
        "data_start_date": "2021-05-07",
        "data_end_date": "2021-05-24",
        "true_start_date": None,
        "true_end_date": None,
    },
    "omasau": {
        "query": "#Omasau OR #OmaGate OR #UmweltsauOma OR #OmaUmweltsau OR conversation_id:1211673284321918977 "
        "OR conversation_id:1210935523545620480",
        "data_start_date": "2019-12-21",
        "data_end_date": "2020-01-07",
        "true_start_date": "2019-12-28",
        "true_end_date": "2019-12-31",
    },
    "helmeLookLikeShit": {
        "query": "#HelmeRettenLeben OR #lookslikeshit OR #saveslifes OR conversation_id:1108842805089177615",
        "disabled": True,
        "data_start_date": "2019-03-14",
        "data_end_date": "2019-03-31",
        "true_start_date": None,
        "true_end_date": None,
    },
    "studentinnenfutter": {
        "query": "#Studentinnenfutter OR Studierendenfutter OR conversation_id:1413204084694437888",
        "disabled": True,
        "data_start_date": "2021-07-01",
        "data_end_date": "2021-07-18",
        "true_start_date": "2021-07-09",
        "true_end_date": "2021-07-09",
    },
    "dbUnternehmerischeEntscheidung": {
        "query": "conversation_id:1359969795068727296 OR conversation_id:1349076313315942404 OR "
        "conversation_id:1359926029909827585",
        "disabled": True,
        "data_start_date": "2021-02-04",
        "data_end_date": "2021-02-21",
        "true_start_date": None,
        "true_end_date": None,
    },
    "thunbergsVsDB": {
        "query": "conversation_id:1206182673888219136",
        "disabled": True,
        "data_start_date": "2019-12-08",
        "data_end_date": "2019-12-25",
        "true_start_date": "2019-12-15",
        "true_end_date": "2019-12-16",
    },
    "kloecknerNestle": {
        "query": "(#Klöckner #Nestle) OR conversation_id:1135553266476040192",
        "data_start_date": "2019-05-27",
        "data_end_date": "2019-06-13",
        "true_start_date": "2019-06-05",
        "true_end_date": "2019-06-07",
    },
    "uefaUngarn": {
        "query": "#MuenchenMachEsTrotzdem OR (#Regenbogen #UEFA) OR (#Regenbogenfarben #UEFA) OR (#AllianzArena #UEFA) "
        "OR (#UEFA #GERHUN) OR (#Regenbogen #EURO2020) OR (#Regenbogenfarben #EURO2020)",
        "data_start_date": "2021-06-14",
        "data_end_date": "2021-07-01",
        "true_start_date": "2021-06-22",
        "true_end_date": "2021-06-23",
    },
    "laschetLacht": {
        "query": "#Laschetlacht OR #LaschetRuecktritt OR #LautGegenLaschet OR conversation_id:1416449185172369411",
        "data_start_date": "2021-07-10",
        "data_end_date": "2021-07-27",
        "true_start_date": "2021-07-17",
        "true_end_date": "2021-07-19",
    },
}


for key in QUERIES:
    # add possibly missing true_dates
    if "true_start_date" not in QUERIES[key] or not QUERIES[key]["true_start_date"]:
        QUERIES[key]["true_start_date"] = QUERIES[key]["data_start_date"]
    if "true_end_date" not in QUERIES[key] or not QUERIES[key]["true_end_date"]:
        QUERIES[key]["true_end_date"] = QUERIES[key]["data_end_date"]

    # convert dates to datetimes
    for entry in [
        "data_start_date",
        "data_end_date",
        "true_start_date",
        "true_end_date",
    ]:
        date = datetime.strptime(QUERIES[key][entry], DATE_FORMAT)
        date = GER_TZ.localize(date)
        day_wrapping_dt = day_wrapping_datetimes(date)
        if entry == "data_start_date" or entry == "true_start_date":
            QUERIES[key][entry] = day_wrapping_dt[0]
        elif entry == "data_end_date" or entry == "true_end_date":
            QUERIES[key][entry] = day_wrapping_dt[1]
        else:
            raise ValueError(f"Entry {entry} was not expected here.")
