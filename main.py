from datetime import date, timedelta

import databutton as db
import entsoe_client as ec
import pandas as pd
import streamlit as st
from entsoe_client.ParameterTypes import *

PRICE_DF = "energy-price-no01"
PRICE_COL = "øre/MWh"
TIME_COL = "hour"
SECRET_NAME = "ensoe-token"

client = ec.Client(db.secrets.get(SECRET_NAME))
parser = ec.Parser


@db.apps.streamlit(route="/home", name="home", cpu=0.5, memory="0.5Gi")
def home():
    df = db.storage.dataframes.get("energy-price-no01")

    st.title(f"Spot Market Index")
    st.subheader(f"Norway East (NO01): {date.today() + timedelta(days=1)}")

    st.bar_chart(df, x=TIME_COL, y=PRICE_COL)

    st.markdown("Data source: [Entsoe](https://transparency.entsoe.eu/)")


@db.jobs.repeat_every(seconds=60 * 60, name="load energy prices")
def load_data():
    date_from = date.today() + timedelta(days=1)
    date_to = date_from + timedelta(days=1)

    time_interval = f"{date_from}T00:00:00.000Z/{date_to}T00:00:00.000Z"

    query = ec.Query(
        documentType=DocumentType("Price Document"),
        in_Domain=Area("NO_1"),
        out_Domain=Area("NO_1"),
        timeInterval=time_interval,
    )
    response = client(query)

    df = parser.parse(response)
    df[PRICE_COL] = df["price.amount"].astype(float) * 10.17 * 100 / 1000
    df[PRICE_COL] = df[PRICE_COL].map(lambda x: round(x, 2))

    df[TIME_COL] = df.index.map(
        lambda t: "{:02d}".format(pd.Timestamp(t).tz_convert("CET").hour)
    )

    df = df[[PRICE_COL, TIME_COL]].reset_index()

    db.storage.dataframes.put(df, PRICE_DF)
