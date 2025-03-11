import pytest
# from app import create_app
from app import create_app

@pytest.fixture
def app():
    #Important for pytest to access the application
    app = create_app()

    app.config.update({
        "TESTING": True,
    })
    with app.app_context():
        yield app

# Run the tests using the command: 
#           pytest
# Or the command below to use tests within a particular package
# pytest <dirName>
