import time, requests

from app.logger import get_global_logger
from app.actions.helper import ActionHelper

def apiGet(executor):
    logger = get_global_logger()
    cookies = executor.COOKIES
    headers  = executor.HEADERS
    timeout  = executor.TIMEOUT
    scrape_content = []

    weblinks = []
    if isinstance(executor.BASE_API, list):
        weblinks = executor.BASE_API
    elif isinstance(executor.BASE_API, dict):
        base_url = executor.BASE_API.get("base_url")
        params = executor.BASE_API.get("params", {})
        weblinks = ActionHelper.build_multiple_urls(base_url, params)
    else:
        logger.error("No valid 'BASE_API' found for weblist action.")
        return scrape_content
    
    logger.info(f"Performing apiget {len(weblinks)} api(s).")
    
    
    session = requests.Session()
    domain  = executor.DOMAIN
    verify = executor.VERIFY_REQUEST
    for name, value in cookies.items():
        session.cookies.set(name, value, domain=domain)
    
    for api_url in weblinks:
        time.sleep(2)
        logger.info(f"Fetching {api_url}")
        resp = session.get(api_url, headers=headers, timeout=timeout,verify=verify)

        if resp.status_code == 200:
            data = resp.json()
            # print("Success! Sample keys:", list(data.keys())[:5])
            # with open("nse_slb_test.json", "w", encoding="utf-8") as f:
            #     json.dump(data, f, indent=2)
            scrape_content.append(data)
            # print("Saved to nse_slb_test.json")
        else:
            logger.error("Failed:", resp.status_code, resp.text[:200])

    executor.ACTION_TYPE = "api_get"
    return scrape_content
    