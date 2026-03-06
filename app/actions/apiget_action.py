import time, requests, json

from app.logger import get_global_logger
from app.actions.helper import ActionHelper

def apiGet(executor):
    logger = get_global_logger()

    # --- executor params ---
    cookies, headers, timeout = executor.COOKIES, executor.HEADERS, executor.TIMEOUT
    domain, verify = executor.DOMAIN, executor.VERIFY_REQUEST
    structure, null_resp = executor.API_RULE, executor.NULL_RESPONSE
    scrape_content = []

    # --- build API links ---
    if isinstance(executor.BASE_API, list):
        weblinks = executor.BASE_API
    elif isinstance(executor.BASE_API, dict):
        base_url = executor.BASE_API.get("base_url")
        params = executor.BASE_API.get("params", {})
        weblinks = ActionHelper.build_multiple_urls(base_url, params)
    else:
        logger.error("No valid 'BASE_API' found for api_get action.")
        return scrape_content

    logger.info(f"Performing api_get on {len(weblinks)} endpoint(s).")

    # --- session & cookies ---
    session = requests.Session()
    for name, value in cookies.items():
        session.cookies.set(name, value, domain=domain)

    # --- loop through APIs ---
    for api_url in weblinks:
        time.sleep(executor.THROTTLE)
        logger.info(f"Fetching {api_url}")

        try:
            resp = session.get(api_url, headers=headers, timeout=timeout, verify=verify)
        except Exception as e:
            logger.error(f"Request failed for {api_url}: {e}")
            scrape_content.append({
                "api_url": api_url,
                "type": "json",
                "data_present": False,
                "status": "network_error",
                "note": str(e)
            })
            continue

        # --- success ---
        if resp.status_code == 200:
            try:
                api_data = resp.json()
            except Exception as e:
                logger.error(f"Invalid JSON from {api_url}: {e}")
                api_data = {"data": [{"symbol": "Null Response from API"}]}
            
            if not api_data or all(v in [[], {}, None] for v in api_data.values()):
                api_data = null_resp

            # api_data.update({"api_url":api_url})
            
            response_block = {
                "api_url": api_url,
                "structure": structure,
                "type": "json",
                "data_present": True,
                "value": api_data
            }

            scrape_content.append(response_block)
            logger.info(f"✅ Appended API data for {api_url}")

        else:
            logger.error(f"Failed ({resp.status_code}) for {api_url}: {resp.text[:150]}")
            scrape_content.append({
                "api_url": api_url,
                "type": "json",
                "data_present": False,
                "status": f"http_{resp.status_code}",
                "note": resp.text[:150]
            })

    executor.ACTION_TYPE = "api_get"
    return scrape_content


# def apiGet(executor):
#     logger = get_global_logger()
#     cookies,headers,timeout = executor.COOKIES,executor.HEADERS,executor.TIMEOUT
#     domain  = executor.DOMAIN
#     verify = executor.VERIFY_REQUEST
#     structure = executor.API_RULE
#     null_resp = executor.NULL_RESPONSE
#     scrape_content = []

#     weblinks = []
#     if isinstance(executor.BASE_API, list): weblinks = executor.BASE_API
#     elif isinstance(executor.BASE_API, dict):
#         base_url = executor.BASE_API.get("base_url")
#         params = executor.BASE_API.get("params", {})
#         weblinks = ActionHelper.build_multiple_urls(base_url, params)
#     else:
#         logger.error("No valid 'BASE_API' found for weblist action.")
#         return scrape_content
    
#     logger.info(f"Performing apiget {len(weblinks)} api(s).")
    
#     session = requests.Session()
#     for name, value in cookies.items():
#         session.cookies.set(name, value, domain=domain)
    
#     for api_url in weblinks:
#         time.sleep(2)
#         logger.info(f"Fetching {api_url}")
#         resp = session.get(api_url, headers=headers, timeout=timeout,verify=verify)

#         if resp.status_code == 200:
#             logger.info("Successful Response 200. Appending data")
#             data = resp.json()
            
#             #null
#             if not data or all(v in [[], {}, None] for v in data.values()):
#                 data.update(null_resp)
                
#             data.update({"api_url":api_url,"structure":structure,"type":"json","data_present":True})
#             scrape_content.append(data)
#         else:
#             logger.error("Failed:", resp.status_code, resp.text[:200])

#     executor.ACTION_TYPE = "api_get"
#     return scrape_content
    