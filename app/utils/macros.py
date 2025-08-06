from datetime import datetime


def _macro_school_year():
    cur_date = datetime.now()
    cur_month = cur_date.month
    if cur_month <= 7:
        return cur_date.year
    else:
        return cur_date.year + 1


def _macro_yyyymmdd():
    cur_date = datetime.now()
    string = cur_date.strftime("%Y%M%D")
    return string


macro_registry = {"SCHOOL_YEAR": _macro_school_year, "YYYYMMDD": _macro_yyyymmdd}
