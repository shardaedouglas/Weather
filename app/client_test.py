import pytest
import json
# from app import create_app
from app import create_app


@pytest.fixture
def client():
    app = create_app()
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_home(client):
    """Test the home route."""
    response = client.get('/')
    assert response.status_code == 200
    #assert response.get_json() == {"message": "Hello, Flask!"}
    
# Run the tests using the command: pytest