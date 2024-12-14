import random
import string

import pytest
from dotenv import load_dotenv


@pytest.fixture(autouse=True, scope="session")
def _load_dotenv():
    load_dotenv()


@pytest.fixture
def random_message():
    return "".join(random.choices(string.ascii_letters + string.digits, k=10))


@pytest.fixture
def get_random_message():
    def _generate_random_str():
        return "".join(random.choices(string.ascii_letters + string.digits, k=10))

    return _generate_random_str
