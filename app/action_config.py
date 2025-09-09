from dataclasses import dataclass, field

@dataclass
class ActionConfig:
    # --- Action Core ---
    ACTION: str                          # The type of action (click, scrape, table, etc.)
    BY: str = "css"                      # Locator strategy
    VALUE: str = None                    # Locator value
    URL: str = "https://tinyurl.com/nothing-borgir"  # Default URL if none provided

    # --- Timing / Wait Handling ---
    DEFAULT_WAIT: int = 2                # Base random wait time
    TIMEOUT: int = 15                    # Max timeout for waits
    WAIT_UNTIL: str = None               # Wait condition (clickable, visible, etc.)
    WAIT_BY: str = None                  # Locator strategy for wait
    WAIT_VALUE: str = None               # Locator value for wait

    # --- Naming Conventions ---
    TABLE_NAME: str = "table"            # Name for table extraction
    HTML_NAME: str = "html"              # Name for raw HTML save
    SCREENSHOT_NAME: str = "screenshot"  # Name for screenshots
    PDF_NAME: str = "webpage_pdf"        # Name for PDF export
    EXPORT_FORMAT: str = None            # Export format (excel, csv, json etc.)
    LOG_MESSAGE: str = "Log Msg For Action Not Attached."

    # --- Scraping / Extraction ---
    ATTRIBUTE: str = None                # Attribute to scrape (e.g. href, src)
    SCRAPE_FIELDS: dict = field(default_factory=dict) # Field mapping for text scrape
    STEPS: list = field(default_factory=list)         # Follow-up actions
    ALLOWED_TABS: list = field(default_factory=list)  # Tabs allowed for action

    # --- Saving / Persistence ---
    CONSOLIDATE_SAVE: bool = False       # Whether to consolidate multiple results
    MULTIPLE: bool = False               # Expect multiple elements
    FILE_SAVE: bool = False              # Whether to save as file (html/pdf/etc.)

    # --- Page / PDF Config ---
    LANDSCAPE: bool = False              # PDF orientation
    PRINT_BACKGROUND: bool = False       # Print CSS backgrounds in PDF

    # --- Window / Navigation ---
    NEW_WINDOW: bool = False             # Open in new browser window
    RETURN_TO_BASE: bool = False         # Return to previous window after action

    # --- Post Init (dependency management) ---
    def __post_init__(self):
        if self.WAIT_BY is None:
            self.WAIT_BY = self.BY
        if self.WAIT_VALUE is None:
            self.WAIT_VALUE = self.VALUE
