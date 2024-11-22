# Import the necessary libraries.
from playwright.async_api import async_playwright
from fastapi import HTTPException, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from bs4 import BeautifulSoup
from PIL import Image
from io import BytesIO
import csv
import requests
import time
import os
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Create an instance of the FastAPI class.
app = FastAPI()

# Configure CORS
origins = ["http://localhost", "http://localhost:8000", "http://localhost:3000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)

# Thread pool for concurrent image downloads
executor = ThreadPoolExecutor()

# Function to download an image and save it to a folder asynchronously
async def download_image_to_folder(url, filename, folder="images"):
    """Download an image from a URL and save it in the specified folder."""
    if not os.path.exists(folder):
        os.makedirs(folder)

    def download_image():
        response = requests.get(url)
        img = Image.open(BytesIO(response.content))
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Get current timestamp
        # Construct the new filename with the timestamp at the end
        img_path = os.path.join(folder, f"{filename}_{timestamp}.png")  # Append timestamp
        img.save(img_path)
        return img_path

    return await asyncio.get_event_loop().run_in_executor(executor, download_image)

# Function to load existing URLs from the metadata CSV file
def load_existing_urls(csv_filename='metadata.csv'):
    """Load existing URLs from the CSV file into a set for quick lookup."""
    existing_urls = set()
    if os.path.exists(csv_filename):
        with open(csv_filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                existing_urls.add(row['post_url'])
    return existing_urls

# Function to save the listings metadata to a CSV file
def save_to_csv(data, csv_filename='metadata.csv'):
    """Save the parsed data to a CSV file."""
    csv_fields = ['image_paths', 'title', 'price', 'post_url', 'location']
    
    # Check if the CSV file already exists
    file_exists = os.path.isfile(csv_filename)
    
    # Open the CSV file in append mode if it exists
    with open(csv_filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=csv_fields)
        
        # Write the header only if the file is new
        if not file_exists:
            writer.writeheader()
        
        for item in data:
            writer.writerow(item)
    
    print(f"Data successfully saved to {csv_filename}")

# Function to crawl Facebook Marketplace and append new listings
@app.get("/crawl_facebook_marketplace")
async def crawl_facebook_marketplace_append(city: str, query: str, max_price: int):
    print(f"City: {city}, Query: {query}, Max Price: {max_price}")

    # Load existing URLs from the CSV file
    existing_urls = load_existing_urls()

    cities = {
        'New York': 'nyc', 'Los Angeles': 'la', 'Las Vegas': 'vegas', 'Chicago': 'chicago',
        'Houston': 'houston', 'San Antonio': 'sanantonio', 'Miami': 'miami', 'Orlando': 'orlando',
        'San Diego': 'sandiego', 'Arlington': 'arlington', 'Manila': 'manila'
    }

    if city in cities:
        city = cities[city]
    else:
        raise HTTPException(404, f'{city} is not supported.')

    # Define the URL to scrape with a 100km radius
    marketplace_url = f'https://www.facebook.com/marketplace/{city}/search/?query={query}&maxPrice={max_price}&radius_km=100'
    initial_url = "https://www.facebook.com/login/device-based/regular/login/"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        # Navigate to Facebook login page
        await page.goto(initial_url)

        # Login to Facebook
        await (await page.wait_for_selector('input[name="email"]')).fill('baevkookie@yahoo.com')
        await (await page.wait_for_selector('input[name="pass"]')).fill('erza22cho')
        await (await page.wait_for_selector('button[name="login"]')).click()
        await asyncio.sleep(5)

        # Navigate to the marketplace URL
        await page.goto(marketplace_url)
        await asyncio.sleep(2)  # Wait for the marketplace to load

        # Scroll down to load more listings until no new listings are loaded
        prev_height = 0
        scroll_count = 0
        while True:
            # Scroll down the page
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)  # Give time to load
            curr_height = await page.evaluate("document.body.scrollHeight")
            
            if curr_height == prev_height or scroll_count > 50:  # Break when no new content is loaded or limit to 50 scrolls
                break
            prev_height = curr_height
            scroll_count += 1

        # Get the page content
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')

        # Parse the listings
        parsed = []
        listings = soup.find_all('div', class_='x9f619 x78zum5 x1r8uery xdt5ytf x1iyjqo2 xs83m0k x1e558r4 x150jy0e x1iorvi4 xjkvuk6 xnpuxes x291uyu x1uepa24')

        limit = 500  # Set the limit to 1000 listings
        count = 0

        for listing in listings:
            if count >= limit:
                break

            try:
                # Get the post URL
                post_url_element = listing.find('a', href=True)
                post_url = f"https://www.facebook.com{post_url_element['href']}" if post_url_element else 'No URL available'
                
                # Check if the URL already exists
                if post_url in existing_urls:
                    print(f"Skipping already existing listing: {post_url}")
                    continue  # Skip already existing listings
                
                # Get the item title
                title_element = listing.find('span', class_='x1lliihq x6ikm8r x10wlt62 x1n2onr6')
                title = title_element.text if title_element else 'No title available'

                # Get the item price
                price_element = listing.find('span', class_='x193iq5w xeuugli x13faqbe x1vvkbs x1xmvt09 x1lliihq x1s928wv xhkezso x1gmr53x x1cpjm7i x1fgarty x1943h6x xudqn12 x676frb x1lkfr7t x1lbecb7 x1s688f xzsf02u')
                price = price_element.text if price_element else 'No price available'

                # Get the item location
                location_element = listing.find('span', class_='x1lliihq x6ikm8r x10wlt62 x1n2onr6 xlyipyv xuxw1ft x1j85h84')
                location = location_element.text if location_element else 'No location available'

                # Navigate to the post URL to scrape images
                image_paths = []
                if post_url != 'No URL available':
                    await page.goto(post_url)
                    await asyncio.sleep(2)

                    # Get all the thumbnails
                    thumbnails = await page.query_selector_all('div[aria-label^="Thumbnail"]')
                    
                    if thumbnails:
                        # Loop through each thumbnail if present
                        for i, thumbnail in enumerate(thumbnails):
                            # Click each thumbnail to load the corresponding image
                            await thumbnail.click()
                            await asyncio.sleep(1)  # Wait for the image to load

                            # Get the current image
                            post_html = await page.content()
                            post_soup = BeautifulSoup(post_html, 'html.parser')
                            current_image_element = post_soup.find('img', class_='x5yr21d xl1xv1r xh8yej3')

                            if current_image_element and current_image_element.has_attr('src'):
                                current_image_url = current_image_element['src']
                                image_filename = f"listing_{count+1}_image_{i+1}.png"
                                image_path = await download_image_to_folder(current_image_url, image_filename)
                                image_paths.append(image_path)

                    else:
                        # If no thumbnails are present, scrape the main image
                        post_html = await page.content()
                        post_soup = BeautifulSoup(post_html, 'html.parser')
                        current_image_element = post_soup.find('img', class_='x5yr21d xl1xv1r xh8yej3')

                        if current_image_element and current_image_element.has_attr('src'):
                            current_image_url = current_image_element['src']
                            image_filename = f"listing_{count+1}_image_1.png"
                            image_path = await download_image_to_folder(current_image_url, image_filename)
                            image_paths.append(image_path)

                # Add the item details to the parsed list
                parsed.append({
                    'image_paths': ', '.join(image_paths),  # Save image paths as a comma-separated string
                    'title': title,
                    'price': price,
                    'post_url': post_url,
                    'location': location
                })
                count += 1

            except Exception as e:
                print(f"Error parsing a listing: {e}")
                continue

        # Close the browser
        await browser.close()

        # Save data to CSV with image paths
        save_to_csv(parsed)

        # Return the parsed data
        return parsed

if __name__ == "__main__":
    uvicorn.run("app:app", host='127.0.0.1', port=8000)
