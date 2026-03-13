
import requests
from bs4 import BeautifulSoup

# URL of Khan Academy homepage
url = "https://docs.github.com/en/get-started/start-your-journey/about-github-and-git"

# Send HTTP GET request
response = requests.get(url)
response.raise_for_status()  # Check for errors

# Parse HTML content
soup = BeautifulSoup(response.text, "html.parser")

# Extract navigation links and their text
nav_links = []
for a_tag in soup.find_all("a", href=True):
    text = a_tag.get_text(strip=True)
    href = a_tag['href']
    if text and href.startswith("/"):  # internal links
        nav_links.append((text, f"{url.rstrip('/')}{href}"))

# Print the extracted links
print("Extracted navigation links:")
for title, link in nav_links:
    print(f"{title}: {link}")

print(f"\nTotal links found: {len(nav_links)}")
