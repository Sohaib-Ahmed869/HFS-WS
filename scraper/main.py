import argparse
import time
import json
import re
import requests
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys

from menu import scrape_menu_for_restaurant
import threading
import queue

class UberEatsScraper:
    def __init__(self):
        self.driver = None
        self.scraped_urls = set()  # Track already scraped URLs

    def setup_driver(self, visible=False):
        """Setup Chrome driver with options"""
        chrome_options = Options()
        if not visible:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        return self.driver

    def close_dialog_if_present(self):
        """Close any dialog that might appear"""
        try:
            close_button = WebDriverWait(self.driver, 2).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="close-button"]'))
            )
            close_button.click()
            time.sleep(0.5)
            return True
        except TimeoutException:
            return False

    def search_postal_code(self, postal_code):
        """Search for the postal code in UberEats"""
        try:
            search_input = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '#location-typeahead-home-input'))
            )
            search_input.click()
            search_input.clear()
            search_input.send_keys(postal_code)

            try:
                suggestion = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '[role="option"]'))
                )
                suggestion.click()
                return True
            except TimeoutException:
                search_input.send_keys(Keys.RETURN)
                return True
        except TimeoutException:
            return False

    def navigate_to_ubereats(self):
        """Navigate to UberEats homepage"""
        self.driver.get("https://www.ubereats.com/fr/feed")
        time.sleep(2)

    def load_existing_urls(self, postal_code):
        """Load existing restaurant URLs to avoid duplicates"""
        filename = f"restaurants_{postal_code}.json"
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                self.scraped_urls = {item['url'] for item in existing_data if 'url' in item}
                print(f"[+] Loaded {len(self.scraped_urls)} existing restaurants")
        except FileNotFoundError:
            print(f"[+] Starting fresh - no existing data found")
            self.scraped_urls = set()

    def save_restaurant_data(self, data, postal_code):
        """Save restaurant data to JSON file immediately"""
        filename = f"restaurants_{postal_code}.json"
        
        try:
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except FileNotFoundError:
                existing_data = []
            
            # Check if URL already exists
            existing_urls = {item['url'] for item in existing_data if 'url' in item}
            if data['url'] not in existing_urls:
                existing_data.append(data)
                
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(existing_data, f, indent=2, ensure_ascii=False)
                
                return True
            else:
                print(f"[!] Duplicate URL skipped: {data['url']}")
                return False
            
        except Exception as e:
            print(f"[!] Error saving data: {e}")
            return False

    def get_info(self, restaurant_url, postal_code, max_menu_items=None):
        """Scrape restaurant information from the info page - WITH MENU INTEGRATION"""
        try:
            self.driver.get(restaurant_url)
            time.sleep(1)
            
            restaurant_data = {"url": restaurant_url, "postal_code": postal_code}
            
            # STEP 1: Get name from restaurant page (before info link)
            try:
                name_selectors = ['h1[class*="hn"][class*="ho"]', 'h1', '[data-testid*="store-name"]', '[class*="store-name"]']
                restaurant_name = "N/A"
                for selector in name_selectors:
                    try:
                        name_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        name_text = name_element.text.strip()
                        if name_text and len(name_text) > 2:
                            restaurant_name = name_text
                            break
                    except:
                        continue
                restaurant_data["name"] = restaurant_name
            except:
                restaurant_data["name"] = "N/A"
            
            # STEP 1.5: SCRAPE MENU ITEMS IN PARALLEL (NEW)
            menu_items = []
            menu_error = None
            
            def scrape_menu_thread():
                nonlocal menu_items, menu_error
                try:
                    print(f"[*] Starting menu scraping for: {restaurant_name}")
                    menu_items = scrape_menu_for_restaurant(self.driver, max_menu_items)
                    print(f"[✓] Menu scraping completed: {len(menu_items)} items")
                except Exception as e:
                    menu_error = str(e)
                    print(f"[!] Menu scraping failed: {e}")
            
            # Start menu scraping in background
            if max_menu_items is None or max_menu_items > 0:
                menu_thread = threading.Thread(target=scrape_menu_thread)
                menu_thread.start()
            
            # STEP 2: Find and click info link - OPTIMIZED
            info_link_found = False
            for attempt in range(3):
                try:
                    info_link = None
                    
                    time.sleep(0.5)
                    
                    try:
                        info_link = self.driver.find_element(By.XPATH, '//a[contains(text(), "Informations") or contains(text(), "informations")]')
                    except:
                        try:
                            info_link = self.driver.find_element(By.XPATH, '//a[contains(@href, "storeInfo") or contains(@href, "info")]')
                        except:
                            try:
                                info_link = self.driver.find_element(By.CSS_SELECTOR, 'a[class*="af"][class*="d3"][class*="db"][class*="e4"][class*="ec"][class*="de"][class*="ee"]')
                            except:
                                self.driver.execute_script("window.scrollBy(0, 300);")
                                time.sleep(0.5)
                                continue
                    
                    if info_link:
                        info_url = info_link.get_attribute("href")
                        self.driver.get(info_url)
                        time.sleep(1.5)
                        
                        for i in range(2):
                            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                            time.sleep(0.5)
                        
                        info_link_found = True
                        break
                        
                except Exception:
                    continue
            
            if not info_link_found:
                print(f"[!] Could not find info link after 3 attempts")
                restaurant_data.update({"email": "N/A", "phone": "N/A", "registration_number": "N/A"})
            else:
                # STEP 3: Extract data from info popup - FASTER
                data_extracted = False
                for attempt in range(3):
                    try:
                        try:
                            spans = self.driver.find_elements(By.CSS_SELECTOR, 'div span')
                            
                            for span_idx, span in enumerate(spans):
                                span_html = span.get_attribute('innerHTML')
                                span_text = span.text.strip()
                                
                                if (span_html and len(span_text) > 50 and 
                                    ('@' in span_text or '+' in span_text) and
                                    any(char.isdigit() for char in span_text)):
                                    
                                    if '<br>' in span_html:
                                        parts = span_html.split('<br>')
                                        clean_lines = []
                                        for part in parts:
                                            clean_part = re.sub(r'<[^>]*>', '', part).strip()
                                            if clean_part:
                                                clean_lines.append(clean_part)
                                    else:
                                        clean_lines = [line.strip() for line in span_text.split('\n') if line.strip()]
                                    
                                    # Extract email
                                    email = "N/A"
                                    for line in clean_lines:
                                        if "@" in line and len(line) < 100:
                                            email = line
                                            break
                                    
                                    # Extract phone
                                    phone = "N/A"
                                    for line in clean_lines:
                                        if "+" in line and len(line) < 50:
                                            phone = line
                                            break
                                    
                                    # Extract registration number
                                    registration = "N/A"
                                    for line in clean_lines:
                                        clean_line = line.replace(' ', '')
                                        if clean_line.isdigit() and len(clean_line) > 6:
                                            registration = clean_line
                                    
                                    if email != "N/A" and phone != "N/A" and registration != "N/A":
                                        restaurant_data["email"] = email
                                        restaurant_data["phone"] = phone
                                        restaurant_data["registration_number"] = registration
                                        print(f"[✓] Data extracted using 'div span' selector (span #{span_idx + 1})")
                                        data_extracted = True
                                        break
                            
                        except:
                            pass
                        
                        if data_extracted:
                            break
                        
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(1)
                        
                    except Exception:
                        time.sleep(0.5)
                        continue
                
                if not data_extracted:
                    restaurant_data.setdefault("email", "N/A")
                    restaurant_data.setdefault("phone", "N/A")
                    restaurant_data.setdefault("registration_number", "N/A")
                    print(f"[!] Could not extract all data after 3 attempts")
            
            # STEP 4: WAIT FOR MENU SCRAPING TO COMPLETE (NEW)
            if max_menu_items is None or max_menu_items > 0:
                print(f"[*] Waiting for menu scraping to complete...")
                menu_thread.join(timeout=30)  # Wait max 30 seconds
                
                if menu_thread.is_alive():
                    print(f"[!] Menu scraping timeout - proceeding without menu")
                    menu_items = []
                elif menu_error:
                    print(f"[!] Menu scraping error: {menu_error}")
                    menu_items = []
            
            # STEP 5: ADD MENU ITEMS TO RESTAURANT DATA (NEW)
            restaurant_data["menu_items"] = menu_items
            restaurant_data["menu_items_count"] = len(menu_items)
            
            print(f"[✓] Restaurant complete: {restaurant_name} | Menu items: {len(menu_items)}")
            
            # Save to JSON file
            self.save_restaurant_data(restaurant_data, postal_code)
            return restaurant_data
            
        except Exception as e:
            print(f"[!] Error in get_info: {e}")
            return None

    def get_restaurant(self, postal_code, max_restaurants=None, max_menu_items=None):
        """Click on each restaurant card and scrape info with limits"""
        try:
            cards = self.driver.find_elements(By.CSS_SELECTOR, 'a[data-testid="store-card"]')
            total_cards = len(cards)
            successful_count = 0
            
            print(f"[+] Found {total_cards} restaurant cards")
            
            for index, card in enumerate(cards):
                try:
                    if max_restaurants and successful_count >= max_restaurants:
                        print(f"[+] Reached limit of {max_restaurants} restaurants")
                        break
                    
                    restaurant_url = card.get_attribute("href")
                    if not restaurant_url:
                        continue
                    
                    if restaurant_url in self.scraped_urls:
                        print(f"[!] {index + 1}/{total_cards} - Already scraped, skipping")
                        continue
                    
                    self.driver.execute_script("window.open(arguments[0]);", restaurant_url)
                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    
                    # Pass menu limit to get_info
                    restaurant_data = self.get_info(restaurant_url, postal_code, max_menu_items)
                    if restaurant_data:
                        successful_count += 1
                        self.scraped_urls.add(restaurant_url)
                        print(f"[✓] {successful_count}/{max_restaurants if max_restaurants else '∞'} - {restaurant_data.get('name', 'N/A')} | Menu: {restaurant_data.get('menu_items_count', 0)} items")
                    else:
                        print(f"[!] {index + 1}/{total_cards} - Failed to extract data")
                    
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])
                    
                    time.sleep(0.3)
                    
                except Exception as e:
                    print(f"[!] Error on card {index + 1}: {e}")
                    try:
                        self.driver.switch_to.window(self.driver.window_handles[0])
                    except:
                        pass
                    continue
            
            print(f"[+] Page complete: {successful_count} new restaurants scraped")
            return successful_count
            
        except Exception as e:
            print(f"[!] Error in get_restaurant: {e}")
            return 0

    def scrape_page(self, postal_code, max_restaurants=None, max_menu_items=None):
        """Main scraping function with restaurant and menu limits"""
        print(f"[*] Starting scraping for postal code: {postal_code}")
        if max_restaurants:
            print(f"[*] Maximum restaurants to scrape: {max_restaurants}")
        if max_menu_items:
            print(f"[*] Maximum menu items per restaurant: {max_menu_items}")
        
        self.load_existing_urls(postal_code)
        
        page_count = 1
        total_scraped = 0
        
        while True:
            print(f"\n[*] Processing page {page_count}")
            
            if max_restaurants and total_scraped >= max_restaurants:
                print(f"[+] Reached target of {max_restaurants} restaurants")
                break
            
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            while True:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.8)
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            remaining = max_restaurants - total_scraped if max_restaurants else None
            
            # Pass both limits to get_restaurant
            scraped_count = self.get_restaurant(postal_code, remaining, max_menu_items)
            total_scraped += scraped_count
            
            print(f"[+] Page {page_count}: {scraped_count} new restaurants | Total: {total_scraped}")
            
            if max_restaurants and total_scraped >= max_restaurants:
                print(f"[✓] Target reached: {total_scraped}/{max_restaurants} restaurants")
                break
            
            try:
                show_more_button = self.driver.find_element(By.CSS_SELECTOR, 'button.ky.br.bo.ds.dk.o5.e8.al.bc.d4.af.o6.o7.j1.o8.o9.oa.gr.gs.ob')
                
                if show_more_button.is_displayed() and show_more_button.is_enabled():
                    print(f"[+] Clicking 'Show more' for page {page_count + 1}")
                    self.driver.execute_script("arguments[0].click();", show_more_button)
                    time.sleep(1.5)
                    page_count += 1
                else:
                    break
                    
            except (NoSuchElementException, Exception):
                print(f"[+] No more pages available")
                break
        
        print(f"[✓] FINAL: {total_scraped} restaurants from {page_count} pages")
        
        return {
            'pages_processed': page_count,
            'restaurants_scraped': total_scraped
        }

    def close_driver(self):
        """Close the browser driver"""
        if self.driver:
            self.driver.quit()


# HIGH-LEVEL FUNCTIONS FOR FLASK APP TO CALL
def perform_search(postal_code, visible=False):
    """Perform postal code search only"""
    start_time = time.time()
    scraper = UberEatsScraper()
    
    try:
        scraper.setup_driver(visible=visible)
        
        page_load_start = time.time()
        scraper.navigate_to_ubereats()
        page_load_time = time.time() - page_load_start
        
        dialog_closed = scraper.close_dialog_if_present()
        
        search_start = time.time()
        search_success = scraper.search_postal_code(postal_code)
        search_time = time.time() - search_start
        
        total_time = time.time() - start_time
        
        return {
            'success': search_success,
            'postal_code': postal_code,
            'timing': {
                'page_load_time': round(page_load_time, 2),
                'search_time': round(search_time, 2),
                'total_time': round(total_time, 2)
            },
            'dialog_closed': dialog_closed,
            'message': 'Search completed' if search_success else 'Search failed'
        }
    
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }
    
    finally:
        scraper.close_driver()


def perform_full_scrape(postal_code, visible=False, max_restaurants=None, max_menu_items=None):
    """Perform search and full restaurant scraping with limits"""
    start_time = time.time()
    scraper = UberEatsScraper()
    
    try:
        scraper.setup_driver(visible=visible)
        
        page_load_start = time.time()
        scraper.navigate_to_ubereats()
        page_load_time = time.time() - page_load_start
        
        dialog_closed = scraper.close_dialog_if_present()
        
        search_start = time.time()
        search_success = scraper.search_postal_code(postal_code)
        search_time = time.time() - search_start
        
        if not search_success:
            return {
                'success': False,
                'error': 'Failed to search postal code'
            }
        
        time.sleep(3)
        
        scraping_start = time.time()
        scrape_results = scraper.scrape_page(postal_code, max_restaurants, max_menu_items)
        scraping_time = time.time() - scraping_start
        
        total_time = time.time() - start_time
        
        return {
            'success': True,
            'postal_code': postal_code,
            'scraping_results': scrape_results,
            'limits': {
                'max_restaurants': max_restaurants,
                'max_menu_items': max_menu_items
            },
            'timing': {
                'page_load_time': round(page_load_time, 2),
                'search_time': round(search_time, 2),
                'scraping_time': round(scraping_time, 2),
                'total_time': round(total_time, 2)
            },
            'dialog_closed': dialog_closed,
            'message': f"Scraped {scrape_results['restaurants_scraped']} restaurants from {scrape_results['pages_processed']} pages",
            'output_file': f"restaurants_{postal_code}.json"
        }
    
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }
    
    finally:
        scraper.close_driver()


# UTILITY FUNCTIONS
def check_api_health(base_url="http://localhost:5000"):
    """Check if the Flask API is running"""
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        if response.status_code == 200:
            return True
    except:
        pass
    return False


# STANDALONE SCRAPING FUNCTION (for direct usage without Flask)
def standalone_scrape(postal_code, visible=False, max_restaurants=None, max_menu_items=None):
    """Run scraping directly without Flask API"""
    
    print("\n##########################################################")
    print("########### UBEREATS STANDALONE SCRAPER ############")
    print("##########################################################\n")
    
    print(f"Starting standalone scraping...")
    print(f"Postal Code: {postal_code}")
    print(f"Visible Mode: {'ON' if visible else 'OFF'}")
    if max_restaurants:
        print(f"Max Restaurants: {max_restaurants}")
    if max_menu_items:
        print(f"Max Menu Items: {max_menu_items}")
    
    # Run the scraping
    result = perform_full_scrape(postal_code, visible, max_restaurants, max_menu_items)
    
    if result.get('success'):
        print(f"\n[✓] Scraping completed successfully!")
        print(f"[+] {result['message']}")
        print(f"[+] Output file: {result['output_file']}")
        
        scraping_results = result['scraping_results']
        print(f"\n[>] Pages Processed: {scraping_results['pages_processed']}")
        print(f"[>] Restaurants Scraped: {scraping_results['restaurants_scraped']}")
        
        timing = result['timing']
        print(f"\n[>] Total Time: {timing['total_time']} seconds")
        
        return True
    else:
        print(f"\n[!] Scraping failed!")
        print(f"[!] Error: {result.get('error', 'Unknown error')}")
        return False


def main():
    """Command line interface for standalone scraping"""
    parser = argparse.ArgumentParser(description='UberEats Scraper - Standalone Mode')
    parser.add_argument('--postal', required=True, help='Postal code to search')
    parser.add_argument('--visible', action='store_true', help='Run browser in visible mode')
    parser.add_argument('--limit', type=int, help='Maximum number of restaurants to scrape')
    parser.add_argument('--menu-limit', type=int, help='Maximum number of menu items per restaurant')
    
    args = parser.parse_args()
    
    success = standalone_scrape(
        args.postal, 
        args.visible, 
        args.limit, 
        args.menu_limit
    )
    
    if success:
        print(f"\n[+] Script completed successfully!")
    else:
        print(f"\n[!] Script failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()