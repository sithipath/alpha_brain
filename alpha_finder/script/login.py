import requests
import json
import os

def brain_login(credential_path=None):
    """
    Logs into the WorldQuant BRAIN platform using credentials from a file.

    By default, this function looks for the .brain_credentials file in the
    same directory as this script (login.py).

    Args:
        credential_path (str, optional): The path to the .brain_credentials file.
                                         If None, it defaults to the 'script' directory.

    Returns:
        requests.Session: An authenticated session object if login is successful,
                          otherwise None.
    """
    # If no path is provided, construct the path to the credentials file
    # assuming it's in the same directory as this script.
    if credential_path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        credential_path = os.path.join(script_dir, '.brain_credentials')

    # Check if the credentials file exists
    if not os.path.exists(credential_path):
        print(f"Error: Credentials file not found at '{os.path.abspath(credential_path)}'")
        print("Please ensure '.brain_credentials' is in the 'script' folder alongside 'login.py'.")
        return None

    # Create a session to persistently store headers and cookies
    s = requests.Session()

    try:
        # Load credentials from the JSON file into the session's auth attribute
        with open(credential_path, 'r') as f:
            s.auth = tuple(json.load(f))
    except (json.JSONDecodeError, TypeError) as e:
        print(f"Error: Could not read or parse the credentials file. Please check its format.")
        print(f"Details: {e}")
        return None

    print("Authenticating with WorldQuant BRAIN...")

    try:
        # Send a POST request to the authentication API endpoint
        response = s.post('https://api.worldquantbrain.com/authentication')

        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()

        print("Authentication successful!")
        # The 's' session object is now authenticated and ready for use.
        return s

    except requests.exceptions.HTTPError as e:
        print(f"Authentication failed. Status code: {e.response.status_code}")
        print(f"Response: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the authentication request: {e}")
        return None