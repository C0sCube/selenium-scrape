# app/actions/inject_script_action.py
from app.logger import get_global_logger
from app.actions.helper import ActionHelper
from app.constants import load_gen_config

def injectScript(executor):
    """
    Execute a JavaScript snippet (either inline via 'script' or predefined via 'script_key')
    in the current Selenium page context.
    """
    logger = get_global_logger()
    driver = executor.driver
    script_key = getattr(executor, "SCRIPT_KEY", None)
    raw_script = getattr(executor, "SCRIPT", None)
    
    generic_actions = load_gen_config()
    scripts = generic_actions.get("scripts",{})

    try:
        # Load predefined script if script_key provided
        if script_key:
            js = scripts.get(script_key)
            if not js:
                logger.warning(f"No predefined script found for key: {script_key}")
                return []
            logger.notice(f"Executing predefined script key: {script_key}")

        elif raw_script:
            js = raw_script
            logger.notice("Executing inline JavaScript snippet.")

        else:
            logger.warning("No 'script' or 'script_key' provided.")
            return []

        driver.execute_script(js)
        logger.save(f"Script executed successfully: {script_key or '[inline script]'}")

        return [ActionHelper.generate_resp_packet(
            name=f"script_{script_key or 'inline'}",
            header="Executed Script",
            value="success",
            type="script"
        )]

    except Exception as e:
        logger.error(f"Script execution failed: {type(e).__name__} - {e}")
        return [ActionHelper.generate_resp_packet(
            name=f"script_{script_key or 'inline'}",
            header="Script Failed",
            value=str(e),
            type="error"
        )]
