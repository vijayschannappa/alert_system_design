# %%
from typing import List
from pathlib import Path
import pandas as pd
from datetime import datetime as dt, time
from matplotlib import pyplot as plt
import itertools as it
import yagmail

plt.style.use("ggplot")

from reconnect import rrf, _sqlsession

print("\n\nStarting")

data_folder = Path(__file__).parent.resolve() / "data/source_diff/"
data_folder.mkdir(exist_ok=True)

plots_folder = Path(__file__).parent.resolve() / "plots/source_diff/"
plots_folder.mkdir(exist_ok=True)


s = rrf.RRFSession.new("read_only")
tbl_name = f"DATA_ACTUAL_PSS_PRO_{dt.now().year}"
query = f"""SELECT DAPP.*, RSM.SUBSTATION_NAME, RSM.ENERGY_TYPE, RSM.CAPACITY FROM {tbl_name} AS DAPP
            LEFT JOIN RSM_SUBSTATION_MASTER AS RSM
            USING (SUBSTATION_ID)
            WHERE TIMESTAMP >= NOW() - INTERVAL 4 HOUR"""

pss = (
    s.session.pull(query=query)
    # .assign(TIMESTAMP=lambda df: pd.to_datetime(df.TIMESTAMP))
    .drop(columns=["AUTO_INDEX"])
)

# %%
# repeating: send every 1h, last3h, 3 repeating_ts
# diff: threshold: 15, last: 3, max
# give all 3 func values in email

# %%

power = (
    pd.pivot_table(
        pss,
        index=[
            "ENERGY_TYPE",
            "CAPACITY",
            "SUBSTATION_ID",
            "SUBSTATION_NAME",
            "TIMESTAMP",
        ],
        columns=["SOURCE_TAG"],
        values=["ATTRIBUTE_1"],
    )
    .reset_index()
    .droplevel(0, axis="columns")
)
# %%
power.columns = [
    "ENERGY_TYPE",
    "CAPACITY",
    "SUBSTATION_ID",
    "SUBSTATION_NAME",
    "TIMESTAMP",
] + list(power.columns)[5:]
power = (
    power.sort_values(["SUBSTATION_ID", "TIMESTAMP"])
    .reset_index(drop=True)
    .assign(IMDAS_SCADA_DIFF=pd.NA)
)


power.head()

# %%
def solar_diff(p: pd.DataFrame) -> pd.DataFrame:
    p["IMDAS_SCADA_DIFF"] = (p["PSS_SCADA_CLT"] - p["PSS_METER_RES"]).abs()

    p["IMDAS_SCADA_DIFF_PCT"] = (
        p["IMDAS_SCADA_DIFF"] / (p["PSS_SCADA_CLT"] + 1e-5)
    ).round(2) * 100

    p_diffed = (
        p.dropna(subset=["IMDAS_SCADA_DIFF"])
        .query('`ENERGY_TYPE` == "SOLAR"')
        .reset_index(drop=True)
    )
    return p_diffed


def cutoff_diffs(
    p: pd.DataFrame,
    thresholds: List[int],
    last_n_hour: List[int],
    thresh_types: List[str],
):
    summary = pd.DataFrame()
    max_ts = p.TIMESTAMP.max()
    for thresh, last, ttype in it.product(thresholds, last_n_hour, thresh_types):
        max_allowed_ts = max_ts - pd.Timedelta(f"{last}H")
        pq = (
            p.query(f'TIMESTAMP >= "{max_allowed_ts}"')
            .groupby(["SUBSTATION_ID", "SUBSTATION_NAME"])
            .agg(DIFF_PCT=("IMDAS_SCADA_DIFF_PCT", ttype))
            .reset_index()
            .query(f"DIFF_PCT >= {thresh}")
            .assign(
                THRESHOLD_PCT=thresh,
                LAST_N_HOURS=last,
                DIFF_FUNC=ttype,
                DIFF_PCT=lambda df: df.DIFF_PCT.round(2),
            )
        )
        summary = summary.append(pq)
    summary = summary.sort_values(
        by=["DIFF_FUNC", "DIFF_PCT", "SUBSTATION_ID"], ascending=False
    ).reset_index(drop=True)
    # summary = summary.groupby(["THRESHOLD_PCT", "LAST_N_HOURS", "DIFF_FUNC"]).agg(
    #     NUM_PSS=("SUBSTATION_ID", "count")
    # )
    return summary


def add_data(p_alerts: pd.DataFrame, p_data: pd.DataFrame):
    # p_ = p_data.loc[
    # ["SUBSTATION_ID", "CAPACITY", "TIMESTAMP", "PSS_SCADA_CLT", "PSS_METER_RES"]
    # ]
    p_ = (
        pd.merge(
            p_data,
            p_alerts,
            how="right",
            left_on=["SUBSTATION_ID", "SUBSTATION_NAME"],
            right_on=["SUBSTATION_ID", "SUBSTATION_NAME"],
        )
        .sort_values(by=["DIFF_FUNC", "DIFF_PCT", "SUBSTATION_ID"], ascending=False)
        .reset_index(drop=True)
    )
    return p_


def save_plot(ss: pd.DataFrame, directory: Path):
    pl1 = ss.plot(
        x="TIMESTAMP", y=["PSS_METER_RES", "PSS_SCADA_CLT"], marker="o", figsize=(12, 6)
    )
    ss_id: str = ss.SUBSTATION_ID.values[0]
    ss_name = ss.SUBSTATION_NAME.values[0]
    threshold = ss.DIFF_PCT.values[0]
    ttypes = ",".join(e for e in ss.DIFF_FUNC.unique())
    pl1.set_title(f"{ss_id} {ss_name} {threshold}% [{ttypes}]")
    pl1.set_ylabel("POWER (MW)")
    f = pl1.get_figure()
    plot_fn = str(directory / f"{ss_id}_{ss_name}.jpg")
    f.savefig(plot_fn)
    ss["PLOT"] = plot_fn
    plt.close()
    return ss


# %%
THRESHOLDS = [15]
LAST_N_HOURS = [3]
DIFF_FUNCS = ["median"]


p_diff = power.pipe(solar_diff)
ps = p_diff.pipe(
    cutoff_diffs,
    thresholds=THRESHOLDS,
    last_n_hour=LAST_N_HOURS,
    thresh_types=DIFF_FUNCS,
)

pe = add_data(ps, p_diff)

# %%
min_ts = pe.TIMESTAMP.min().to_pydatetime()
max_ts = pe.TIMESTAMP.max().to_pydatetime()
plot_dir = plots_folder / f"{min_ts:%d %b} {min_ts:%H:%M}_to_{max_ts:%H:%M}/"
plot_dir.mkdir(exist_ok=True)

pss_to_alert = pe.groupby("SUBSTATION_ID", sort=False).apply(
    save_plot, directory=plot_dir
)
summary_fn = (
    data_folder / f"scada_imdas_diff_{min_ts:%Y-%m-%d %H:%M} to {max_ts:%H:%M}.csv"
)
ps.to_csv(summary_fn)

# %%
SUBJECT = f"DATA ALERTS: SCADA METER DIFFERENCE ({min_ts:%H:%M} to {max_ts:%H:%M})"
TO = [
    "atindra.nair@reconnectenergy.com",
    "nilesh.sharma@reconnectenergy.com",
    "indraja.yadav@reconnectenergy.com",
    "dinesh.kumar@reconnectenergy.com",
    "siddharth.chib@reconnectenergy.com",
]

START = [
    "Hi team,",
    "The following substations have significant differences in SCADA/METER data:",
    "",
    "",
]
END = ["", "Summary file attached."]


def send_mail(pss_alerts: pd.DataFrame, p_summary_fn: Path):
    c = {"user": "ops_support@reconnectenergy.com", "password": "opsfs123@"}
    acc = yagmail.SMTP(**c)
    acc.login()
    images = [yagmail.inline(p) for p in list(pss_alerts.PLOT.unique())]
    contents = START + images + END
    acc.send(to=TO, subject=SUBJECT, contents=contents, attachments=p_summary_fn)
    acc.close()
    print(f"Email sent for {len(images)} PSS.")


send_mail(pss_to_alert, summary_fn)
