import shutil
import tempfile

import pytest


@pytest.fixture
def mock_dir():
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path)