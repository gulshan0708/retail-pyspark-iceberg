"""Unit tests for ProfileTransformer."""

from datetime import date

import pytest
from chispa import assert_df_equality

from src.transform.profile import ProfileTransformer

CUSTOMERS = (
    "customer_id int, first_name string, last_name string, "
    "email string, signup_date date, country string"
)


@pytest.fixture
def customers(spark):
    return spark.createDataFrame(
        [
            (1, "Anna", "Kim", "anna.kim1@example.com", date(2024, 1, 15), "US"),
            (2, "Ben", None, None, date(2024, 3, 2), "UK"),
        ],
        CUSTOMERS,
    )


def by_id(df):
    return {row.customer_id: row for row in df.collect()}


def test_derives_name_email_and_date_columns(customers):
    row = by_id(ProfileTransformer().transform(customers))[1]
    assert row.full_name == "Anna Kim"
    assert row.initials == "AK"
    assert row.email_domain == "example.com"
    assert row.email_user == "anna.kim1"
    assert row.signup_month == "2024-01"
    assert row.signup_weekday == "Monday"  # 15 January 2024 was a Monday


def test_nulls_are_handled_differently_by_concat_ws_and_concat(customers):
    row = by_id(ProfileTransformer().transform(customers))[2]
    assert row.full_name == "Ben"       # concat_ws skips the null last name
    assert row.initials is None         # concat returns null if any input is null
    assert row.email_domain is None
    assert row.email_user is None
    assert row.signup_weekday == "Saturday"


def test_signup_ts_is_a_timestamp(customers):
    result = ProfileTransformer().transform(customers)
    assert dict(result.dtypes)["signup_ts"] == "timestamp"


def test_selectexpr_version_gives_the_same_result(customers):
    transformer = ProfileTransformer()
    assert_df_equality(
        transformer.transform(customers),
        transformer.transform_with_sql(customers),
        ignore_row_order=True,
        ignore_nullable=True,
    )