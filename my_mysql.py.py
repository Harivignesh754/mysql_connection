import mysql.connector
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure logging
logging.basicConfig(filename='log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# Set up the MySQL connection
mysql_host = "localhost"
mysql_user = "root"
mysql_password = "air@135"
mysql_database = "officesupplyy"

# Establish the database connection
try:
    db_connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = db_connection.cursor()
except Exception as e:
    logging.error(f"Error connecting to MySQL: {e}")
    raise  # Halt if connection fails

# Fetch data from the 'inputs' table
try:
    cursor.execute("SELECT * FROM inputs")
    input_data = cursor.fetchall()
except Exception as e:
    logging.error(f"Error fetching data from MySQL: {e}")
    input_data = []

# Column indices for accessing data from 'inputs'
COLUMN_INDICES = {
    'ID': 1,
    'Brand': 2,
    'BrandAlias': 3,
    'MPN': 4,
    'UPC': 5,
    'ProductName': 6,
    'ProductPrice': 7,
    'Domain': 8,
    'ProductNameAlias': 9,
    'Timestamp': 10
}

# Insert data into 'outputs' table
def insert_into_outputs(cursor, row, compare_url):
    try:
        cursor.execute(
            "INSERT INTO outputs (ID, Brand, BrandAlias, MPN, UPC, ProductName, ProductPrice, Domain, ProductNameAlias, url, Timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                row[COLUMN_INDICES['ID']],
                row[COLUMN_INDICES['Brand']],
                row[COLUMN_INDICES['BrandAlias']],
                row[COLUMN_INDICES['MPN']],
                row[COLUMN_INDICES['UPC']],
                row[COLUMN_INDICES['ProductName']],
                row[COLUMN_INDICES['ProductPrice']],
                row[COLUMN_INDICES['Domain']],
                row[COLUMN_INDICES['ProductNameAlias']],
                compare_url,
                datetime.now()  # Current timestamp
            )
        )
    except Exception as e:
        logging.error(f"Error inserting into outputs: {e}")

# Create a function to retrieve URLs via Selenium
def get_comparison_urls(driver, product_name):
    compare_urls = []
    try:
        driver.get("https://www.google.com/shopping")
        search_box = driver.find_element(By.NAME, 'q')
        search_box.send_keys(product_name)
        search_box.submit()

        # Wait for search results to load
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, "iXEZD")))

        # Get comparison URLs (up to 5)
        compare_prices_links = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, "//a[contains(text(), 'Compare prices')]"))
        )

        for link in compare_prices_links[:5]:
            compare_urls.append(link.get_attribute("href"))
    
    except Exception as e:
        logging.error(f"Error retrieving comparison URLs for '{product_name}': {e}")

    return compare_urls

# Set up Selenium WebDriver
chrome_service = Service("C:/Users/Admin/Desktop/5compare page url/chromedriver.exe")
driver = webdriver.Chrome(service=chrome_service)

# Process each row in the 'inputs' table
for row in input_data:
    product_name = row[COLUMN_INDICES['ProductName']]  # Get product name

    # Get URLs for this product
    compare_urls = get_comparison_urls(driver, product_name)

    # Insert each comparison URL into 'outputs'
    for url in compare_urls:
        insert_into_outputs(cursor, row, url)

# Commit changes and close database connections
db_connection.commit()
cursor.close()
db_connection.close()

# Close the Selenium WebDriver
driver.quit()

print("Data processing completed and uploaded to MySQL database.")
