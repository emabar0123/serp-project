from time import sleep, time
import datetime


def wait_until(delegate, timeout: int):
    end = time() + timeout

    while time() < end:
        if delegate():
            return True
        else:
            sleep(0.1)
    return False


def parse_date(date):
    try:
        if "PM" in date:
            try:
                return datetime.datetime.strptime(date, "%m/%d/%Y %H:%M:%S.%f PM").strftime("%d/%m/%Y %H:%M:%S.%f")
            except:
                return datetime.datetime.strptime(date, "%m/%d/%Y %H:%M:%S PM").strftime("%d/%m/%Y %H:%M:%S")
        elif "AM" in date:
            try:
                return datetime.datetime.strptime(date, "%m/%d/%Y %H:%M:%S.%f AM").strftime("%d/%m/%Y %H:%M:%S.%f")
            except:
                return datetime.datetime.strptime(date, "%m/%d/%Y %H:%M:%S AM").strftime("%d/%m/%Y %H:%M:%S")
        elif "UTC" in date:
            try:
                return datetime.datetime.strptime(date, "%m/%d/%Y %H:%M:%S.%f UTC").strftime("%d/%m/%Y %H:%M:%S.%f")
            except:
                return datetime.datetime.strptime(date, "%m/%d/%Y %H:%M:%S UTC").strftime("%d/%m/%Y %H:%M:%S")
        elif '-' in date:
            return datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M:%S")
        else:
            try:
                return datetime.datetime.strptime(date, "%m/%d/%Y %H:%M:%S.%f").strftime("%d/%m/%Y %H:%M:%S.%f")
            except:
                return datetime.datetime.strptime(date, "%m/%d/%Y %H:%M:%S").strftime("%d/%m/%Y %H:%M:%S")
    except:
        return ""
