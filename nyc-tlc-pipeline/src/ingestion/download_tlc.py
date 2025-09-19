import requests
import os
import sys

# Path setup for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config import url, filename
   
try:
    os.makedirs("nyc-tlc-pipeline/data/raw", exist_ok=True)
    print("Directory created/exists")
       
    print("Starting download...")
    response = requests.get(url)
    response.raise_for_status()  # Raises an exception for bad status codes
       
    print(f"Download complete. Content size: {len(response.content)} bytes")
       
    with open(f"nyc-tlc-pipeline/data/raw/{filename}", "wb") as f:
        f.write(response.content)
           
    # Verify file was written
    file_size = os.path.getsize(f"nyc-tlc-pipeline/data/raw/{filename}")
    print(f"File saved successfully! Size: {file_size} bytes")
       
except requests.exceptions.RequestException as e:
    print(f"Network error: {e}")
except OSError as e:
    print(f"File system error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")