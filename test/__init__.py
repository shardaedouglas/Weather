# For Pytest

import pytest

@pytest.fixture
def client():
    from app import app
    app.app.config['TESTING'] = True
    return app.app.test_client()