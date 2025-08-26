import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options

from app.utils import Helper
import random, time, math, ssl

ssl._create_default_https_context = ssl._create_stdlib_context

def human_scroll(driver, total_scroll=2000, step=50):
    for y in range(0, total_scroll, step):
        driver.execute_script(f"window.scrollTo(0, {y});")
        time.sleep(random.uniform(0.05, 0.3))


def human_move_mouse(driver, start=(0, 0), end=(300, 300), steps=30):
    actions = ActionChains(driver)
    x1, y1 = start
    x2, y2 = end
    for t in range(steps):
        progress = t / steps
        x = x1 + (x2 - x1) * progress + random.uniform(-2, 2)
        y = y1 + (y2 - y1) * progress + math.sin(progress * math.pi) * 20
        actions.move_by_offset(int(x), int(y))
    actions.perform()


def human_click(elem):
    time.sleep(random.uniform(0.2, 1.5))
    elem.click()

profile_path = Helper.load_json("paths.json").get("profile_path")
profile_dir = Helper.load_json("paths.json").get("profile_name")

options = uc.ChromeOptions()
# options.add_argument(f"--user-data-dir={profile_path}")
# options.add_argument(f"--profile-directory={profile_dir}")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--start-maximized")
options.add_argument("--disable-extensions")

driver = uc.Chrome(options=options)

driver.get("https://www.unionbankofindia.co.in/en/details/rate-of-interest")

# human_scroll(driver, total_scroll=1500, step=70)
# human_move_mouse(driver, start=(0, 0), end=(200, 200), steps=40)

try:
    driver.implicitly_wait(30)
    table = driver.find_element(By.TAG_NAME, "table")
    human_click(table) # Just demo click
    print("Table found:", table.get_attribute("outerHTML")[:200])
except Exception as e:
    print("No table found:", e)

time.sleep(5)
driver.quit()