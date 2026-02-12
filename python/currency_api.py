import os, calendar, datetime as dt
import requests
import psycopg2
from psycopg2.extras import execute_values

BASE = "https://api.frankfurter.app"

def month_range(year: int, month: int):
    start = dt.date(year, month, 1)
    end = dt.date(year, month, calendar.monthrange(year, month)[1])
    return start, end
