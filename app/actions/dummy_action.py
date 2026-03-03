import time, requests

from app.logger import get_global_logger
from app.actions.helper import ActionHelper

def dummyTable(executor):
    logger = get_global_logger()
    logger.info(f"dummy performed")
    return [
        ActionHelper.generate_resp_packet(
            value="<table><td><tr>====================</tr><tr>====================</tr><tr>====================</tr></td></table>",
            type="table_html",
        )
    ]