from wikipulse.db import sqlalchemy_url


def test_adds_psycopg_driver():
    assert sqlalchemy_url("postgresql://u:p@h:5432/d") == "postgresql+psycopg://u:p@h:5432/d"
    assert sqlalchemy_url("postgres://u:p@h/d?sslmode=require") == (
        "postgresql+psycopg://u:p@h/d?sslmode=require"
    )


def test_leaves_explicit_driver_alone():
    assert sqlalchemy_url("postgresql+psycopg://u@h/d") == "postgresql+psycopg://u@h/d"
