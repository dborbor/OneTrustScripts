import os
import yaml
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

def substitute_env_vars(data):
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = substitute_env_vars(value)  # Recursive call
    elif isinstance(data, str) and '$' in data:
        data = os.path.expandvars(data)
    return data

def load_config(config_path):
    """
    Loads constants from a YAML configuration file.
    Uses `load_dotenv()` to load environment variables and substitutes them in the YAML file.
    """
    with open(config_path, 'r') as f:
        try:
            configuration = yaml.safe_load(f)

            # Substitute environment variables
            configuration = substitute_env_vars(configuration)  # Start recursion
            return configuration
        except yaml.YAMLError as e:
            print(f"Error loading YAML configuration: {e}")
            return None

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full path to your YAML file (adjust if needed)
configuration_path = os.path.join(current_dir, 'config.yaml')

config = load_config(configuration_path)

if config:
    # Access constants and assign to variables with the names we desire
    onetrust_config = config.get('onetrust', {})  # Get the OneTrust section, default to empty dict if not found
    ONETRUST_HOSTNAME = onetrust_config.get('hostname')
    ONETRUST_VERSION = onetrust_config.get('version')
    ONETRUST_HEADERS = onetrust_config.get('headers')
    DEFAULT_OWNER_ID = onetrust_config.get('default_owner_id')
    DEFAULT_CATEGORY = onetrust_config.get('default_category')

    confluence_config = config.get('confluence', {})
    CONFLUENCE_USERNAME = confluence_config.get('username')
    CONFLUENCE_PASSWORD = confluence_config.get('password')
    CONFLUENCE_URL = confluence_config.get('url')
    CONFLUENCE_SPACE = confluence_config.get('space')

    timeouts_config = config.get('timeouts', {})
    TIMEOUT = timeouts_config.get('http_request')

    filepaths_config = config.get('filepaths', {})
    SHAREPOINT_VENDORS_PATH_MACOS = filepaths_config.get('sharepoint_vendors_macos')
    SHAREPOINT_VENDORS_PATH_WINDOWS = filepaths_config.get('sharepoint_vendors_windows')
    SHAREPOINT_ASSETS_PATH_MACOS = filepaths_config.get('sharepoint_assets_macos')
    SHAREPOINT_ASSETS_PATH_WINDOWS = filepaths_config.get('sharepoint_assets_windows')
    SHAREPOINT_AI_PATH_MACOS = filepaths_config.get('sharepoint_ai_macos')
    SHAREPOINT_AI_PATH_WINDOWS = filepaths_config.get('sharepoint_ai_windows')
    SHAREPOINT_OFFLINE_SW_PATH_MACOS = filepaths_config.get('sharepoint_offline_sw_macos')
    SHAREPOINT_OFFLINE_SW_PATH_WINDOWS = filepaths_config.get('sharepoint_offline_sw_windows')
