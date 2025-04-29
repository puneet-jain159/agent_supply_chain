import json
import os

# Function to save config to JSON file
def save_config(catalog_name, db_name):
    """Save catalog and database names to a config JSON file in DBFS"""
    config = {
        "catalog_name": catalog_name,
        "db_name": db_name
    }
    
    # Save to DBFS
    config_path = "../_resources/supply_chain_config.json"
    with open(config_path, "w") as f:
        json.dump(config, f)
    
    print(f"Config saved to: {config_path}")
    return config_path

# Function to get config from JSON file or widgets
def get_config():
    """Read config from JSON file"""
    config_path = "../_resources/supply_chain_config.json"
    
    # Try to read from config file
    try:
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config = json.load(f)
            print(f"Config loaded from: {config_path}")
            return config["catalog_name"], config["db_name"]
        else:
            raise FileNotFoundError(f"Config file not found at: {config_path}")
    except Exception as e:
        error_message = f"Error: {str(e)}. Please run the 01_Introduction_And_Setup.py notebook first to set up the catalog and schema."
        raise RuntimeError(error_message)