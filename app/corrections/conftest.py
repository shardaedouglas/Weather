import pytest
# from app import create_app
from app import __init__

@pytest.fixture
def app():
    app = create_app()
    return app

# Run the tests using the command: pytest
# Or the command below to use tests within a particular package
# pytest --pyargs mypkg