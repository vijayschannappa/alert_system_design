# %%
from typing import List
from pathlib import Path
import pandas as pd
from datetime import datetime as dt, time
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
import itertools as it
import yagmail

plt.style.use("ggplot")

from reconnect import rrf, _sqlsession

# %%
print("\n\nStarting Repeat Alerter")

data_folder = Path(__file__).parent.resolve() / "data/repeats/"
data_folder.mkdir(exist_ok=True)

plots_folder = Path(__file__).parent.resolve() / "plots/repeats/"
plots_folder.mkdir(exist_ok=True)

# %%

s = rrf.RRFSession.new("read_only")
tbl_name = f"DATA_ACTUAL_PSS_PRO_{dt.now().year}"
query = f"""SELECT DAPP.*, RSM.SUBSTATION_NAME, RSM.ENERGY_TYPE, RSM.CAPACITY FROM {tbl_name} AS DAPP
            LEFT JOIN RSM_SUBSTATION_MASTER AS RSM
            USING (SUBSTATION_ID)
            WHERE TIMESTAMP >= NOW() - INTERVAL 4 HOUR"""

pss = s.session.pull(query=query).drop(columns=["AUTO_INDEX"]).round({"ATTRIBUTE_1": 2})

# %%
def remove_solar_night(p: pd.DataFrame, start: time, end: time) -> pd.DataFrame:
    _power_ts = p.TIMESTAMP.dt.time

    _is_night = (_power_ts <= start) | (_power_ts >= end)
    _is_solar = p.ENERGY_TYPE == "SOLAR"
    _is_solar_night = _is_night & _is_solar

    return p.loc[~_is_solar_night]


def only_repeated(p: pd.DataFrame, n_ts: int) -> pd.DataFrame:
    _repeated_power = (p.ATTRIBUTE_1 != p.ATTRIBUTE_1.shift()).cumsum()
    _repeated_power_groups = p.ATTRIBUTE_1.groupby(_repeated_power)

    _num_repeated_ts = _repeated_power_groups.transform("size")

    pr = p.assign(REPEAT_GROUP=_repeated_power, NUM_REPEATS=_num_repeated_ts)
    pr = pr.loc[pr.NUM_REPEATS >= n_ts]

    # try filter
    # check for non-consecutive ts?
    return pr


def _run_group_check(p: pd.DataFrame, n_ts: int):
    assert p.ATTRIBUTE_1.nunique() == 1
    assert p.NUM_REPEATS.nunique() == 1

    assert p.shape[0] == p.NUM_REPEATS.values[0]
    assert p.shape[0] >= n_ts


def _run_checks(p: pd.DataFrame, n_ts: int):
    p.groupby(p.index).apply(_run_group_check, n_ts=n_ts)
    return p


# %%
def get_repeats(pss_df: pd.DataFrame, last_hours, min_repeats, last_ts):
    pss_repeats = pd.DataFrame()

    for last_h, min_ts in it.product(last_hours, min_repeats):
        if min_ts > last_h:
            continue

        min_allowed_ts = last_ts - pd.Timedelta(last_h, "H")

        gd = (
            pss_df.query(f"TIMESTAMP >= '{min_allowed_ts}'")
            .pipe(remove_solar_night, start=_DAY_START, end=_DAY_END)
            .groupby(["SUBSTATION_ID", "SOURCE_TAG"])
            .apply(only_repeated, n_ts=min_ts)
            .reset_index(drop=True)
            .set_index(["SUBSTATION_ID", "SOURCE_TAG", "REPEAT_GROUP"])
            .pipe(_run_checks, n_ts=min_ts)
        )

        gs = (
            gd.groupby(gd.index)
            .count()
            .describe()
            .loc[["count"], ["NUM_REPEATS"]]
            .rename(columns={"NUM_REPEATS": "NUM_ALERTS"})
            .assign(LAST_HOURS=last_h, MIN_REPEATING_TS=min_ts)
        )

        pss_repeats = pss_repeats.append(gd)
    pss_repeats = pss_repeats.reset_index().sort_values(
        ["NUM_REPEATS", "SUBSTATION_ID"], ascending=False
    )
    return pss_repeats


_DAY_START = time(6, 30, 0)
_DAY_END = time(17, 45, 0)

# repeating: send every 1h, last3h, 3 repeating_ts
LAST_N_HOUR = 4
MIN_REPEATS = 4


pss_power = pss.drop(
    columns=["ATTRIBUTE_2", "ATTRIBUTE_3", "ATTRIBUTE_4", "ATTRIBUTE_5"]
)
last_ts = pss.TIMESTAMP.max()

repeats = get_repeats(pss_power, [LAST_N_HOUR], [MIN_REPEATS], last_ts)
repeats_all = (
    pd.merge(
        pss_power,
        repeats.loc[
            :,
            ["SUBSTATION_ID", "SOURCE_TAG", "TIMESTAMP", "NUM_REPEATS", "REPEAT_GROUP"],
        ].drop_duplicates(),
        how="left",
        on=["SUBSTATION_ID", "SOURCE_TAG", "TIMESTAMP"],
        suffixes=(None, "_REPEATED"),
    )
    .groupby(["SUBSTATION_ID", "SOURCE_TAG"])
    .filter(lambda df: df.loc[df.NUM_REPEATS.notna()].shape[0] > 0)
    .groupby(["SUBSTATION_ID", "SOURCE_TAG"])
    .filter(lambda df: df.ATTRIBUTE_1.loc[df.NUM_REPEATS.notna()].iloc[0] != 0.0)
    .sort_values(["NUM_REPEATS", "SUBSTATION_ID", "SOURCE_TAG"], ascending=False)
    .reset_index(drop=True)
)

min_ts = repeats.TIMESTAMP.min().to_pydatetime()
max_ts = repeats.TIMESTAMP.max().to_pydatetime()


# %%
def save_plot(pss_df: pd.DataFrame, directory: Path):
    ss_id = pss_df.SUBSTATION_ID.values[0]
    ss_name = pss_df.SUBSTATION_NAME.values[0]
    source_tag = pss_df.SOURCE_TAG.values[0]
    num_repeats = pss_df.NUM_REPEATS.values[0]

    plot_name = f"{ss_id}_{ss_name}: {source_tag} ({num_repeats} repeats)"
    # make plot
    pl1 = pss_df.plot(x="TIMESTAMP", y=["ATTRIBUTE_1"], marker="o", figsize=(12, 6))

    # set timestamp ticks
    pl1.set_xticks(pss_df.TIMESTAMP.values)
    pl1.set_xticklabels(
        pss_df.TIMESTAMP.apply(lambda t: t.to_pydatetime().strftime("%H:%M"))
    )

    # set labels
    pl1.set_title(plot_name)
    pl1.set_ylabel("POWER (MW)")
    pl1.set_xlabel(f"Timestamp on {pss_df['TIMESTAMP'].iloc[0].strftime('%B %d')}")

    # add spans and lines
    pl1.axhline(color="blue", alpha=0.3)
    pl1.legend([source_tag])

    # get figure
    f = pl1.get_figure()
    f.autofmt_xdate()

    plot_fn = str(directory / f"{plot_name}.jpg")

    f.savefig(plot_fn)

    pss_df["PLOT"] = plot_fn
    return pss_df


plot_dir = plots_folder / f"{min_ts:%d %b %H:%M} to {max_ts:%H:%M}"
plot_dir.mkdir(exist_ok=True)


repeats_all = repeats_all.groupby(["SUBSTATION_ID", "SOURCE_TAG"], sort=False).apply(
    save_plot, directory=plot_dir
)

# %%

summary = repeats_all.loc[
    :, ["SUBSTATION_ID", "SUBSTATION_NAME", "SOURCE_TAG", "NUM_REPEATS"]
].drop_duplicates()
summary_fn = data_folder / f"{min_ts:%d %b %H:%M} to {max_ts:%H:%M}.csv"
summary.to_csv(summary_fn, index=False)
summary

# %%
SUBJECT = f"DATA ALERTS: REPEATED TIMESTAMPS ({min_ts:%H:%M} to {max_ts:%H:%M})"
TO = [
    "atindra.nair@reconnectenergy.com",
    "nilesh.sharma@reconnectenergy.com",
    "indraja.yadav@reconnectenergy.com",
    "dinesh.kumar@reconnectenergy.com",
    "siddharth.chib@reconnectenergy.com",
]

START = ["Hi team,", "The following substations have sent repeated Power Data:", "", ""]
END = ["", "Thank you"]


def send_mail(pss_alerts: pd.DataFrame, p_summary_fn: Path):
    c = {"user": "ops_support@reconnectenergy.com", "password": "opsfs123@"}
    acc = yagmail.SMTP(**c)
    acc.login()
    images = [yagmail.inline(p) for p in list(pss_alerts.PLOT.unique())]
    contents = START + images + END
    acc.send(to=TO, subject=SUBJECT, contents=contents, attachments=p_summary_fn)
    acc.close()
    print(f"Email sent for {len(images)} PSS.")


send_mail(repeats_all, summary_fn)

