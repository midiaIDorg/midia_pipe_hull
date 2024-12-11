import json
import sqlite3

import click
import pandas as pd


def get_scheme(path):
    with sqlite3.connect(str(path)) as conn:
        return pd.read_sql("SELECT * FROM DiaFrameMsMsWindows", conn)


@click.command(context_settings={"show_default": True})
@click.argument("dataset_analysis_tdf")
@click.argument("calibration_analysis_tdf")
@click.argument("report_output")
def assert_dataset_and_calibration_comply(
    dataset_analysis_tdf: str,
    calibration_analysis_tdf: str,
    report_output: str,
) -> None:
    dataset_scheme = get_scheme(dataset_analysis_tdf)
    calibration_scheme = get_scheme(calibration_analysis_tdf)

    report: dict = {
        "dataset": {
            "file": dataset_analysis_tdf,
            "min_collision_energy": dataset_scheme.CollisionEnergy.min(),
        },
        "calibration": {
            "file": calibration_analysis_tdf,
            "min_collision_energy": calibration_scheme.CollisionEnergy.min(),
            "constant_collision_energy": bool(
                (
                    calibration_scheme.CollisionEnergy
                    == calibration_scheme.CollisionEnergy.iloc[0]
                ).all()
            ),
        },
        "schemes_aggree": all(
            dataset_scheme.drop(columns=["CollisionEnergy"])
            == calibration_scheme.drop(columns=["CollisionEnergy"])
        ),
    }

    if (
        report["dataset"]["min_collision_energy"]
        < report["calibration"]["min_collision_energy"]
    ):
        warn_message = f"""
        \n\n\n
        WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
        \n\n\n
        DATASET
        {dataset_analysis_tdf}
        HAS SOME ENERGY COLLISIONS LOWER THAN CALIBRATION
        {calibration_analysis_tdf}
        \n\n\n
        """
        raise ValueError(warn_message)

    if not report["schemes_aggree"]:
        warn_message = f"""
        \n\n\n
        WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
        \n\n\n
        DATASET
        {dataset_analysis_tdf}
        AND CALIBRATION
        {calibration_analysis_tdf}
        HAVE DIFFERENT WINDOW SCHEMES
        \n\n\n
        """
        raise ValueError(warn_message)

    with open(report_output, "w") as f:
        json.dump(report, f, indent=4)
