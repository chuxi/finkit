from argparse import Namespace

import pytest
from datetime import timedelta, timezone
from finkit import cron, utils

utils.logging_config("../logging.yml")


@pytest.mark.skip
def test_cron():
    args = Namespace(config_file="./cron_config.yml")
    cron.cron(args)


@pytest.mark.skip
def test_exec_method():
    cron._exec_method(["cron", "test_sleep", "MySleeper", "sleep"])
    cron._exec_method(["cron", "test_sleep", "static_sleep"], args={"k1": "v1"})
    cron._exec_method(["cron", "test_sleep", "static_sleep_v2"], args={"k1": "v1", "k3": "v3"})

