import requests, subprocess, sys, certifi, warnings, urllib3
from app.utils import Helper

warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)


DRIVER_PATH = Helper.load_json('paths.json').get("driver_path","")

def get_latest_chromedriver_version():
    url = "https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json"
    try:
        response = requests.get(url, verify=certifi.where())
        response.raise_for_status()
    except requests.exceptions.SSLError:
        print("SSL verification failed. Trying insecure connection...")
        response = requests.get(url, verify=False)
        response.raise_for_status()
    data = response.json()
    return data["channels"]["Stable"]["version"]


def get_local_chromedriver_version():
    try:
        # path = r'C:\Users\rando\chromedriver-win64\chromedriver.exe'
        result = subprocess.run([DRIVER_PATH, "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            return None
        # chromedriver 126.0.6478.126
        return result.stdout.strip().split()[1]
    except FileNotFoundError:
        return None

def main():
    latest = get_latest_chromedriver_version()
    local = get_local_chromedriver_version()

    if local is None:
        print("ChromeDriver not installed. You're not even in the game.")
        sys.exit(1)

    print(f"Latest: {latest}, Installed: {local}")
    if local != latest:
        print("Update required! Your ChromeDriver is a sad fossil.")
    else:
        print("You're up to date. Bask in the glory of mediocrity.")

if __name__ == "__main__":
    main()
