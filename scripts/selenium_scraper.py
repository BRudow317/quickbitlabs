from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

def scrape_rendered_html(url, output_file):
    """
    Scrapes the fully rendered HTML from a URL using a headless Chrome browser.
    """
    print(f"Setting up browser to scrape: {url}")

    # Set up Chrome options for headless mode (no GUI)
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Initialize the driver using webdriver_manager to automatically handle the driver executable
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.get(url)

        # Wait for a few seconds to ensure JavaScript finishes rendering
        driver.implicitly_wait(10)

        # Get the fully rendered HTML
        rendered_html = driver.page_source

        # Save to file
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(rendered_html)
        
        print(f"Success! Rendered HTML saved to: {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")
        
    finally:
        # Always close the browser
        driver.quit()

if __name__ == "__main__":
    target_url = "https://www.markdownguide.org/basic-syntax/#overview"
    scrape_rendered_html(target_url, "markdown_guide_rendered.html")