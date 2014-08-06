import pdb
import pytest
import sys


def pytest_addoption(parser):
    global attr
    parser.addoption("--all", action="store_true",
                     help="run all tests")
    attr = pytest.mark.attr


def pytest_runtest_setup(item):
    # pdb.set_trace()
    if 'attr' in item.keywords and not item.config.getoption("--all"):
        pytest.skip('slow')
