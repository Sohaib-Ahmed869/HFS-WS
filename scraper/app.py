import argparse
import time
import json
import re
import requests
import sys
import os
from urllib.parse import urlparse
import difflib
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from menu import scrape_menu_for_restaurant, detect_establishment_type_from_items
import threading
import queue

class UberEatsScraper:
    def __init__(self):
        self.driver = None
        self.scraped_urls = set()
        self.scraped_store_names = set()  # ADD THIS LINE
        self.wait = None
        self.establishments_data = []

    def setup_driver(self, visible=False):
        """Setup Chrome driver with EC2-optimized options"""
        chrome_options = Options()
        
        if not visible:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-features=TranslateUI")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
        
        chrome_options.add_argument("--memory-pressure-off")
        chrome_options.add_argument("--max_old_space_size=4096")

        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 15)
        self.driver.set_page_load_timeout(30)
        
        self.driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        return self.driver

    def robust_click(self, element_locator, timeout=15, scroll_first=True):
        """ENHANCED: Robust clicking method that handles overlays and interception"""
        if isinstance(element_locator, tuple):
            by, value = element_locator
            element = None
        else:
            element = element_locator
            by, value = None, None
        
        for attempt in range(5):
            try:
                # Get the element if we have locator
                if by and value:
                    element = self.wait.until(EC.presence_of_element_located((by, value)))
                
                if not element:
                    print(f"[!] Element not found for clicking")
                    return False
                
                # Scroll element into view first
                if scroll_first:
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", element)
                    time.sleep(0.8)  # Increased wait after scroll
                
                # Method 1: Wait for element to be clickable and try standard click
                try:
                    clickable_element = WebDriverWait(self.driver, 8).until(EC.element_to_be_clickable(element))
                    clickable_element.click()
                    print(f"[✓] Standard click successful on attempt {attempt + 1}")
                    return True
                except ElementClickInterceptedException as e:
                    print(f"[!] Click intercepted on attempt {attempt + 1}: {str(e)[:100]}...")
                    # Don't return here, continue to JavaScript click
                except TimeoutException:
                    print(f"[!] Element not clickable within timeout on attempt {attempt + 1}")
                    # Continue to JavaScript click
                except Exception as e:
                    print(f"[!] Standard click failed on attempt {attempt + 1}: {e}")
                    # Continue to JavaScript click
                
                # Method 2: JavaScript click (reliable for overlays)
                try:
                    print(f"[*] Trying JavaScript click on attempt {attempt + 1}")
                    self.driver.execute_script("arguments[0].click();", element)
                    print(f"[✓] JavaScript click successful on attempt {attempt + 1}")
                    return True
                except Exception as e:
                    print(f"[!] JavaScript click failed on attempt {attempt + 1}: {e}")
                
                # Method 3: ActionChains click
                try:
                    print(f"[*] Trying ActionChains click on attempt {attempt + 1}")
                    ActionChains(self.driver).move_to_element(element).click().perform()
                    print(f"[✓] ActionChains click successful on attempt {attempt + 1}")
                    return True
                except Exception as e:
                    print(f"[!] ActionChains click failed on attempt {attempt + 1}: {e}")
                
                # Method 4: Force click with JavaScript (alternative approach)
                try:
                    print(f"[*] Trying force JavaScript click on attempt {attempt + 1}")
                    self.driver.execute_script("""
                        arguments[0].focus();
                        arguments[0].click();
                        var event = new MouseEvent('click', {
                            view: window,
                            bubbles: true,
                            cancelable: true
                        });
                        arguments[0].dispatchEvent(event);
                    """, element)
                    print(f"[✓] Force JavaScript click successful on attempt {attempt + 1}")
                    return True
                except Exception as e:
                    print(f"[!] Force JavaScript click failed on attempt {attempt + 1}: {e}")
                    
            except Exception as e:
                if attempt == 4:  # Last attempt
                    print(f"[!] All click methods failed after {attempt + 1} attempts: {e}")
                    return False
                
                print(f"[!] Attempt {attempt + 1} failed, retrying...")
                # Wait before retry
                time.sleep(1.5)
                # Try to dismiss overlays before next attempt
                self.dismiss_overlays()  
        return False

    def dismiss_overlays(self):
        """ENHANCED: Dismiss common overlays that might block clicks"""
        print("[*] Attempting to dismiss overlays...")
        # Specific overlay selectors - caused mainly after deployment
        overlay_selectors = [
            'button[data-testid="close-button"]',
            '[data-testid="close-modal"]',
            '.modal-close',
            '[aria-label="Close"]',
            '[aria-label="Fermer"]', 
            '[data-baseweb="typo-paragraphsmall"]',  
            'p[data-baseweb="typo-paragraphsmall"]', 
            '.overlay-close',
            '[role="dialog"] button',
            '[role="modal"] button',
            'button[class*="close"]',
            'button[aria-label*="close"]',
            'button[aria-label*="Close"]',
            '[data-testid*="dismiss"]',
            '[data-testid*="close"]',
            '.cookie-banner button',
            '.notification-banner button',
            '.popup-close',
            '[class*="banner"] button',
            '[class*="toast"] button'
        ]
        overlays_dismissed = 0
        
        for selector in overlay_selectors:
            try:
                overlays = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for overlay in overlays:
                    try:
                        if overlay.is_displayed() and overlay.is_enabled():
                            print(f"[*] Found visible overlay: {selector}")
                            # Try JavaScript click on overlay
                            self.driver.execute_script("arguments[0].click();", overlay)
                            overlays_dismissed += 1
                            time.sleep(0.8)
                            print(f"[✓] Dismissed overlay: {selector}")
                            break  # Only dismiss one overlay per selector
                    except Exception as e:
                        print(f"[!] Could not dismiss overlay {selector}: {e}")
                        continue
            except Exception:
                continue
        
        # Try pressing ESC key multiple times
        try:
            body = self.driver.find_element(By.TAG_NAME, 'body')
            for _ in range(3):
                body.send_keys(Keys.ESCAPE)
                time.sleep(0.5)
            print(f"[*] Pressed ESC key 3 times")
        except Exception as e:
            print(f"[!] Could not press ESC: {e}")
        
        # Try clicking outside any potential modal
        try:
            self.driver.execute_script("""
                var event = new MouseEvent('click', {
                    view: window,
                    bubbles: true,
                    cancelable: true,
                    clientX: 10,
                    clientY: 10
                });
                document.body.dispatchEvent(event);
            """)
            time.sleep(0.5)
            print(f"[*] Clicked outside to dismiss modals")
        except Exception as e:
            print(f"[!] Could not click outside: {e}")
        
        if overlays_dismissed > 0:
            print(f"[✓] Successfully dismissed {overlays_dismissed} overlays")
        else:
            print(f"[*] No overlays found to dismiss")
        
        return overlays_dismissed

    def close_dialog_if_present(self):
        """Close any dialog that might appear"""
        try:
            # Try multiple close button selectors
            close_selectors = [
                'button[data-testid="close-button"]',
                'button[aria-label="Close"]',
                'button[aria-label="Fermer"]',
                '[data-testid="close-modal"]',
                '[role="dialog"] button[aria-label*="close"]'
            ]
            
            for selector in close_selectors:
                try:
                    close_button = WebDriverWait(self.driver, 3).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    if self.robust_click(close_button):
                        print(f"[✓] Closed dialog using selector: {selector}")
                        time.sleep(1)
                        return True
                except TimeoutException:
                    continue
                except Exception as e:
                    print(f"[!] Error with close selector {selector}: {e}")
                    continue
            
            return False
        except Exception as e:
            print(f"[!] Error in close_dialog_if_present: {e}")
            return False

    def search_postal_code(self, postal_code):
        """ENHANCED: Search for postal code with better error handling"""
        try:
            print(f"[*] Starting postal code search for: {postal_code}")
            # Wait for page to be fully loaded
            WebDriverWait(self.driver, 15).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            time.sleep(2)
            # Dismiss any initial overlays
            self.dismiss_overlays()
            
            search_input_locator = (By.CSS_SELECTOR, '#location-typeahead-home-input')
            
            for attempt in range(5):  # Increased attempts
                try:
                    print(f"[*] Search attempt {attempt + 1}/5")
                    # Wait for search input to be present
                    search_input = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located(search_input_locator)
                    )
                    print(f"[✓] Found search input element")
                    # Scroll to search input
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", search_input)
                    time.sleep(1.5)
                    # Dismiss overlays again before clicking
                    self.dismiss_overlays()
                    # Try to click the search input using robust method
                    print(f"[*] Attempting to click search input...")
                    if self.robust_click(search_input, scroll_first=False):  # Already scrolled
                        print(f"[✓] Successfully clicked search input on attempt {attempt + 1}")
                        break
                    else:
                        print(f"[!] Failed to click search input on attempt {attempt + 1}")
                        if attempt < 4: 
                            time.sleep(2)
                            continue
                        else:
                            return False
                    
                except TimeoutException:
                    print(f"[!] Search input not found on attempt {attempt + 1}")
                    if attempt < 4:
                        time.sleep(2)
                        continue
                    else:
                        return False
                except Exception as e:
                    print(f"[!] Error on search attempt {attempt + 1}: {e}")
                    if attempt < 4:
                        time.sleep(2)
                        self.dismiss_overlays()
                        continue
                    else:
                        return False
            
            # Clear and enter postal code
            try:
                # Clear input field
                search_input.clear()
                time.sleep(0.5)
                
                # Alternative clearing method
                search_input.send_keys(Keys.CONTROL + "a")
                search_input.send_keys(Keys.DELETE)
                time.sleep(0.5)
                
                # Enter postal code
                search_input.send_keys(postal_code)
                print(f"[✓] Entered postal code: {postal_code}")
                time.sleep(2)  # Wait for suggestions to appear
                
            except Exception as e:
                print(f"[!] Error entering postal code: {e}")
                return False

            # Try to click suggestion
            try:
                print(f"[*] Looking for suggestion dropdown...")
                suggestion = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '[role="option"]'))
                )
                print(f"[✓] Found suggestion option")
                
                if self.robust_click(suggestion):
                    print(f"[✓] Successfully clicked suggestion")
                    time.sleep(3)
                    return True
                else:
                    print(f"[!] Failed to click suggestion, trying Enter key")
                    search_input.send_keys(Keys.RETURN)
                    time.sleep(3)
                    return True
                    
            except TimeoutException:
                print(f"[!] No suggestion found, trying Enter key")
                try:
                    search_input.send_keys(Keys.RETURN)
                    time.sleep(3)
                    return True
                except Exception as e:
                    print(f"[!] Error pressing Enter: {e}")
                    return False
                    
        except Exception as e:
            print(f"[!] Critical error in search_postal_code: {e}")
            return False

    def navigate_to_ubereats(self):
        """Navigate to UberEats homepage with enhanced loading"""
        try:
            print("[*] Navigating to UberEats...")
            self.driver.get("https://www.ubereats.com/fr/feed")
            
            # Wait for page to load completely
            WebDriverWait(self.driver, 20).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            time.sleep(3)
            # Waiting for body to be present
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Dismissing any overlays/popups
            self.dismiss_overlays()
            print("[✓] Successfully navigated to UberEats")
            
        except Exception as e:
            print(f"[!] Error navigating to UberEats: {e}")
            raise

    def load_existing_urls(self, postal_code):
        """Load existing establishment URLs AND store names to avoid duplicates"""
        restaurant_filename = f"restaurants_{postal_code}.json"
        store_filename = f"stores_{postal_code}.json"
        
        try:
            with open(restaurant_filename, 'r', encoding='utf-8') as f:
                restaurant_data = json.load(f)
                restaurant_urls = {item['url'] for item in restaurant_data if 'url' in item}
                self.scraped_urls.update(restaurant_urls)
                print(f"[+] Loaded {len(restaurant_urls)} existing restaurants")
        except FileNotFoundError:
            print(f"[+] No existing restaurant data found")
        
        try:
            with open(store_filename, 'r', encoding='utf-8') as f:
                store_data = json.load(f)
                store_urls = {item['url'] for item in store_data if 'url' in item}
                # ADD STORE NAME TRACKING
                store_names = {self.normalize_store_name(item.get('name', '')) for item in store_data if item.get('name') and item.get('name') != 'N/A'}
                
                self.scraped_urls.update(store_urls)
                self.scraped_store_names.update(store_names)
                print(f"[+] Loaded {len(store_urls)} existing stores")
                print(f"[+] Loaded {len(store_names)} existing store names")
        except FileNotFoundError:
            print(f"[+] No existing store data found")
        
        print(f"[+] Total existing URLs loaded: {len(self.scraped_urls)}")
        print(f"[+] Total existing store names loaded: {len(self.scraped_store_names)}")

    def normalize_store_name(self, name):
        """Normalize store names for comparison"""
        if not name or name == 'N/A':
            return ""
        
        # Convert to lowercase and remove common variations
        normalized = name.lower().strip()
        
        # Remove common suffixes/prefixes
        suffixes_to_remove = [
            'sprint', 'express', 'city', 'market', 'super', 'hyper',
            'proximité', 'contact', 'shop', 'store', 'supermarché'
        ]
        
        for suffix in suffixes_to_remove:
            if normalized.endswith(f' {suffix}'):
                normalized = normalized.replace(f' {suffix}', '')
            if normalized.startswith(f'{suffix} '):
                normalized = normalized.replace(f'{suffix} ', '')
        
        # Remove extra spaces
        normalized = ' '.join(normalized.split())
        
        return normalized

    def is_store_already_scraped(self, establishment_name, establishment_type):
        """Check if a store with similar name was already scraped"""
        if establishment_type != "store":
            return False
            
        if not establishment_name or establishment_name == 'N/A':
            return False
            
        normalized_name = self.normalize_store_name(establishment_name)
        
        if normalized_name in self.scraped_store_names:
            print(f"[!] Store already scraped with similar name: {establishment_name} (normalized: {normalized_name})")
            return True
            
        return False

    def detect_establishment_type(self, establishment_url):
        """FIXED: More conservative approach to detect stores vs restaurants"""
        try:
            # Method 1: Check for very specific store indicators in URL
            url_lower = establishment_url.lower()
            # Only the most reliable store indicators
            definite_store_indicators = [
                'carrefour', 'franprix', 'monoprix', 'casino', 'lidl',
                'aldi', 'leclerc', 'intermarche', 'auchan', 'cora',
                'picard', 'metro', 'costco', 'supermarche', 'supermarket'
            ]
            for indicator in definite_store_indicators:
                if indicator in url_lower:
                    print(f"[*] DEFINITE store detected by URL: {indicator}")
                    return "store"
            
            # Method 2: Check page title for grocery-specific terms
            try:
                page_title = self.driver.title.lower()
                grocery_titles = [
                    'supermarché', 'supermarket', 'épicerie', 'courses',
                    'grocery', 'livraison de courses'
                ]
                
                for title_keyword in grocery_titles:
                    if title_keyword in page_title:
                        print(f"[*] Store detected by page title: {title_keyword}")
                        return "store"
            except:
                pass
            
            # Method 3: Count carousel buttons 
            try:
                carousel_buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button[data-testid="next-arrow-carousel"]')
                carousel_count = len(carousel_buttons)
                
                print(f"[*] Found {carousel_count} carousel buttons")
                
                # Only classify as store if there are MANY carousels (8+ is more reliable)
                if carousel_count >= 8:
                    print(f"[*] Store detected by high carousel count: {carousel_count}")
                    return "store"
                elif carousel_count >= 5:
                    # Additional check for stores with 5-7 carousels
                    try:
                        page_source = self.driver.page_source.lower()
                        if any(term in page_source for term in ['produits frais', 'épicerie', 'courses en ligne']):
                            print(f"[*] Store detected by carousel count + content: {carousel_count}")
                            return "store"
                    except:
                        pass
            except:
                pass
            
            # Method 4: Check for specific grocery keywords in page content
            try:
                page_source = self.driver.page_source.lower()
                strong_grocery_keywords = [
                    'livraison de courses', 'courses en ligne', 'produits frais',
                    'épicerie en ligne', 'supermarché en ligne', 'grocery delivery'
                ]
                
                strong_matches = sum(1 for keyword in strong_grocery_keywords if keyword in page_source)
                
                if strong_matches >= 2:
                    print(f"[*] Store detected by strong content analysis: {strong_matches} matches")
                    return "store"
            except:
                pass
            
            # DEFAULT: Classify as restaurant (be conservative)
            print(f"[*] No strong store indicators - classifying as RESTAURANT")
            return "restaurant"
            
        except Exception as e:
            print(f"[!] Error detecting establishment type: {e}")
            return "restaurant"
        
    def safe_click_button(self, button):
        """Safely click a button with multiple methods"""
        try:
            # Method 1: Standard click
            try:
                button.click()
                return True
            except Exception:
                pass
            
            # Method 2: JavaScript click
            try:
                self.driver.execute_script("arguments[0].click();", button)
                return True
            except Exception:
                pass
            
            # Method 3: Action chains
            try:
                from selenium.webdriver.common.action_chains import ActionChains
                ActionChains(self.driver).move_to_element(button).click().perform()
                return True
            except Exception:
                pass
            
            return False
            
        except Exception:
            return False

    def remove_duplicate_buttons(self, buttons):
        """Remove duplicate buttons based on location and attributes"""
        try:
            unique_buttons = []
            seen_signatures = set()
            
            for button in buttons:
                try:
                    # Create a signature for the button
                    location = button.location
                    size = button.size
                    signature = f"{location['x']},{location['y']},{size['width']},{size['height']}"
                    
                    if signature not in seen_signatures:
                        unique_buttons.append(button)
                        seen_signatures.add(signature)
                        
                except Exception:
                    continue
            
            return unique_buttons
            
        except Exception:
            return buttons

    def navigate_single_carousel(self, button):
        """Navigate a single carousel and collect items"""
        try:
            items = []
            clicks = 0
            max_clicks = 10
            
            while clicks < max_clicks:
                try:
                    # Check if button is still valid
                    if not (button.is_enabled() and button.is_displayed()):
                        break
                    
                    # Click the button
                    if not self.safe_click_button(button):
                        break
                    
                    time.sleep(2)  # Wait for new content
                    clicks += 1
                    
                    # Extract new items
                    new_items = self.extract_all_store_items()
                    if new_items:
                        items.extend(new_items)
                        print(f"[+] Click {clicks}: found {len(new_items)} items")
                    else:
                        print(f"[*] Click {clicks}: no new items")
                        break
                    
                except Exception as e:
                    print(f"[!] Error on click {clicks}: {e}")
                    break
            
            return items
            
        except Exception as e:
            print(f"[!] Single carousel navigation failed: {e}")
            return []


    def enhanced_page_loading(self):
        """Load all possible content without carousel navigation"""
        try:
            # Initial scroll to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
            # Progressive scrolling to load dynamic content
            for i in range(8):  # More thorough scrolling
                # Scroll to different positions
                scroll_position = (i + 1) * (1.0 / 8)  # 12.5%, 25%, 37.5%, etc.
                self.driver.execute_script(f"window.scrollTo(0, {scroll_position} * document.body.scrollHeight);")
                time.sleep(1.5)  # Wait for content to load
            
            # Final scroll to bottom and back to top
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
            print(f"[✓] Enhanced page loading complete")
            
        except Exception as e:
            print(f"[!] Enhanced page loading failed: {e}")

    def navigate_carousels_and_extract(self, initial_items, max_items=None):
        """Only navigate carousels if initial extraction wasn't sufficient"""
        try:
            all_items = list(initial_items)  # Copy initial items
            
            # Find carousel buttons with multiple selectors
            carousel_selectors = [
                'button[data-testid="next-arrow-carousel"]',
                'button[data-testid*="next"]',
                'button[aria-label*="next"]',
                'button[aria-label*="suivant"]',
                'button[class*="next"]',
                '[data-testid*="carousel"] button'
            ]
            
            carousel_buttons = []
            for selector in carousel_selectors:
                try:
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    carousel_buttons.extend(buttons)
                except Exception:
                    continue
            
            # Remove duplicates
            unique_buttons = self.remove_duplicate_buttons(carousel_buttons)
            print(f"[+] Found {len(unique_buttons)} carousel buttons")
            
            # Navigate each carousel
            for idx, button in enumerate(unique_buttons):
                try:
                    print(f"[*] Navigating carousel {idx + 1}/{len(unique_buttons)}")
                    new_items = self.navigate_single_carousel(button)
                    
                    if new_items:
                        # Add new items that we don't already have
                        existing_texts = {item['text'] for item in all_items}
                        unique_new_items = [item for item in new_items if item['text'] not in existing_texts]
                        all_items.extend(unique_new_items)
                        print(f"[+] Carousel {idx + 1} added {len(unique_new_items)} new items")
                    
                except Exception as e:
                    print(f"[!] Error navigating carousel {idx + 1}: {e}")
                    continue
            
            # Process all collected items
            products = self.process_store_items(all_items, max_items)
            return products
            
        except Exception as e:
            print(f"[!] Carousel navigation failed: {e}")
            return self.process_store_items(initial_items, max_items)

    def scrape_store_carousels(self, max_items=None):
        """SIMPLIFIED: Try full page extraction first, then carousels if needed"""
        try:
            print(f"[*] Starting GENERIC store scraping...")
            
            # Step 1:  page loading to get all content
            print(f"[*] Loading all page content...")
            self.enhanced_page_loading()
            
            # Step 2: Extract all available content
            print(f"[*] Extracting all available store items...")
            all_items = self.extract_all_store_items()
            print(f"[+] Found {len(all_items)} total items")
            
            # Step 3: If we got good results, skip carousel navigation
            if len(all_items) >= 10:  # Reasonable threshold
                print(f"[*] Good content found ({len(all_items)} items), skipping carousel navigation")
                products = self.process_store_items(all_items, max_items)
                print(f"[✓] Generic store scraping complete: {len(products)} unique products")
                return products
            
            # Step 4: If not enough content, try carousel navigation
            print(f"[*] Not enough content found, trying carousel navigation...")
            carousel_items = self.navigate_carousels_and_extract(all_items, max_items)
            
            print(f"[✓] Enhanced store scraping complete: {len(carousel_items)} unique products")
            return carousel_items
            
        except Exception as e:
            print(f"[!] Store carousel scraping failed: {e}")
            return []
        
    def extract_all_store_items(self):
        """GENERIC: Extract store items focusing on span text within containers"""
        try:
            items = []
            # Method 1: Target the specific structure 
            try:
                # Find containers with the specific data-testid
                containers = self.driver.find_elements(By.CSS_SELECTOR, 'div[data-testid="store-item-thumbnail-label"]')
                print(f"[*] Found {len(containers)} store-item containers")
                
                for container in containers:
                    try:
                        # Look for span with rich-text inside each container
                        spans = container.find_elements(By.CSS_SELECTOR, 'span[data-testid="rich-text"]')
                        
                        for span in spans:
                            text_content = span.text.strip()
                            if text_content and len(text_content) > 5:  # Minimum length filter
                                items.append({
                                    'text': text_content,
                                    'html': span.get_attribute('innerHTML'),
                                    'source': 'specific_span'
                                })
                    except Exception:
                        continue
                        
            except Exception as e:
                print(f"[!] Specific span extraction failed: {e}")
            
            # Method 2: Generic approach - find all spans with data-testid="rich-text"
            if not items:
                try:
                    print(f"[*] Trying generic rich-text spans...")
                    all_rich_spans = self.driver.find_elements(By.CSS_SELECTOR, 'span[data-testid="rich-text"]')
                    print(f"[*] Found {len(all_rich_spans)} rich-text spans")
                    
                    for span in all_rich_spans:
                        try:
                            text_content = span.text.strip()
                            if text_content and len(text_content) > 5:
                                items.append({
                                    'text': text_content,
                                    'html': span.get_attribute('innerHTML'),
                                    'source': 'generic_rich_text'
                                })
                        except Exception:
                            continue
                            
                except Exception as e:
                    print(f"[!] Generic rich-text extraction failed: {e}")
            
            # Method 3: Even more generic - all spans with meaningful text
            if not items:
                try:
                    print(f"[*] Trying all spans...")
                    all_spans = self.driver.find_elements(By.TAG_NAME, 'span')
                    print(f"[*] Found {len(all_spans)} total spans")
                    
                    for span in all_spans[:500]:  # Limit for performance
                        try:
                            text_content = span.text.strip()
                            # Only basic length and UI filtering
                            if (text_content and 
                                len(text_content) > 10 and 
                                len(text_content) < 200 and
                                not self.is_ui_text(text_content)):
                                
                                items.append({
                                    'text': text_content,
                                    'html': span.get_attribute('innerHTML'),
                                    'source': 'generic_span'
                                })
                        except Exception:
                            continue
                            
                except Exception as e:
                    print(f"[!] Generic span extraction failed: {e}")
            
            print(f"[✓] Extracted {len(items)} items from spans")
            return items
            
        except Exception as e:
            print(f"[!] Error extracting store items: {e}")
            return []


    def process_store_items(self, all_items, max_items=None):
        """GENERIC: Process items without hardcoded filtering"""
        try:
            products = []
            processed_descriptions = set()
            
            for item in all_items:
                if max_items and len(products) >= max_items:
                    break
                
                text = item['text']
                
                # Very basic filtering - just length and UI text
                description = self.clean_text(text)
                
                if (len(description) > 10 and  # Minimum meaningful length
                    len(description) < 300 and  # Maximum reasonable length
                    not self.is_ui_text(description) and  # Not UI element
                    description not in processed_descriptions):  # Not duplicate
                    
                    processed_descriptions.add(description)
                    
                    product = {
                        "description": description
                    }
                    
                    products.append(product)
            
            # Remove any remaining duplicates by description
            unique_products = []
            seen_descriptions = set()
            
            for product in products:
                desc = product.get('description', '')
                if desc and desc not in seen_descriptions:
                    unique_products.append(product)
                    seen_descriptions.add(desc)
            
            return unique_products
            
        except Exception as e:
            print(f"[!] Error processing store items: {e}")
            return []

    def quick_click(self, element):
        """FAST: Quick click method for carousel navigation"""
        try:
            # Try JavaScript click first (fastest and most reliable)
            self.driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            try:
                # Fallback to standard click
                element.click()
                return True
            except Exception:
                return False

    def extract_store_products(self, max_items=None, processed_descriptions=None):
        """SIMPLIFIED: Extract store products without price checks"""
        if processed_descriptions is None:
            processed_descriptions = set()
        products = []
        
        try:
            # Method 1: Extract from page source using regex
            page_source = self.driver.page_source
            
            # Look for product descriptions
            description_patterns = [
                r'"description"\s*:\s*"([^"]{20,300})"',  # Product descriptions
                r'"subtitle"\s*:\s*"([^"]{20,300})"',     # Product subtitles
                r'"longDescription"\s*:\s*"([^"]{20,300})"'  # Long descriptions
            ]
            
            for pattern in description_patterns:
                matches = re.findall(pattern, page_source, re.IGNORECASE)
                
                for match in matches:
                    if max_items and len(products) >= max_items:
                        break
                    
                    description = self.clean_text(match)
                    
                    # Basic validation: length and not UI text
                    if (len(description) > 20 and 
                        not self.is_ui_text(description) and
                        description not in processed_descriptions):
                        
                        processed_descriptions.add(description)
                        
                        # For stores: description only
                        product = {
                            "description": description
                        }
                        
                        products.append(product)
                
                if products:
                    break
            
            # Method 2: DOM extraction if regex failed
            if len(products) == 0:
                print("[*] Regex extraction failed, trying DOM extraction...")
                elements = self.driver.find_elements(By.CSS_SELECTOR, 'div[data-testid*="store-item"], div[data-testid*="product"], div[class*="product"], div[class*="item"]')
                
                for element in elements:
                    if max_items and len(products) >= max_items:
                        break
                    
                    try:
                        element_text = element.text.strip()
                        
                        if not element_text or len(element_text) < 20:
                            continue
                        
                        if element_text in processed_descriptions:
                            continue
                        
                        # Parse element text to extract description
                        description = self.parse_store_element_text(element_text)
                        
                        if (description and 
                            len(description) > 20 and 
                            not self.is_ui_text(description)):
                            
                            processed_descriptions.add(element_text)
                            
                            product = {
                                "description": description
                            }
                            
                            products.append(product)
                            
                    except Exception:
                        continue
            
            return products
            
        except Exception as e:
            print(f"[!] Store product extraction failed: {e}")
            return []

    def parse_store_element_text(self, element_text):
        """Parse store element text to extract meaningful description"""
        try:
            lines = [line.strip() for line in element_text.split('\n') if line.strip()]
            
            # Find the best description line (skip UI text)
            for line in lines:
                if (len(line) > 20 and 
                    not self.is_ui_text(line)):
                    return line
            
            # If no good single line, try combining meaningful lines
            meaningful_lines = []
            for line in lines:
                if (len(line) > 10 and 
                    not self.is_ui_text(line)):
                    meaningful_lines.append(line)
            
            if meaningful_lines:
                return " ".join(meaningful_lines[:2])
            
            return ""
            
        except Exception:
            return ""

    def is_ui_text(self, text):
        """Check if text is UI-related (buttons, actions, etc.)"""
        if not text:
            return False
        
        ui_keywords = [
            'ajouter', 'add', 'commander', 'order', 'voir plus', 'show more',
            'disponible', 'available', 'en stock', 'in stock', 'select',
            'choisir', 'options', 'quantity', 'quantité', 'personnaliser',
            'customize', 'modify', 'modifier', 'delete', 'supprimer',
            'click here', 'cliquez ici', 'buy now', 'acheter maintenant',
            'add to cart', 'ajouter au panier', 'livraison', 'delivery',
            'retrait', 'pickup', 'commander maintenant', 'order now'
        ]
        
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in ui_keywords)

    def clean_text(self, text):
        """Enhanced text cleaning"""
        if not text:
            return ""
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Decode HTML entities
        html_entities = {
            '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', 
            '&apos;': "'", '&nbsp;': ' ', '&euro;': '€', '&copy;': '©', 
            '&reg;': '®', '&trade;': '™', '&hellip;': '...', '&mdash;': '—',
            '&ndash;': '–', '&lsquo;': ''', '&rsquo;': ''', '&ldquo;': '"',
            '&rdquo;': '"', '&bull;': '•', '&middot;': '·'
        }
        
        for entity, replacement in html_entities.items():
            text = text.replace(entity, replacement)
        
        # Remove extra whitespace
        text = ' '.join(text.split()).strip()
        
        return text

    def get_info(self, establishment_url, postal_code, max_menu_items=None):
        """STEP 1: Scrape establishment information"""
        try:
            self.driver.get(establishment_url)
            time.sleep(2)
            
            # Dismiss any overlays on the establishment page
            self.dismiss_overlays()
            
            establishment_data = {
                "url": establishment_url, 
                "postal_code": postal_code
            }
            
            # Get establishment name
            try:
                name_selectors = [
                    'h1[class*="hn"][class*="ho"]', 
                    'h1', 
                    '[data-testid*="store-name"]', 
                    '[data-testid*="shop-name"]',
                    '[class*="store-name"]'
                ]
                establishment_name = "N/A"
                for selector in name_selectors:
                    try:
                        name_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        name_text = name_element.text.strip()
                        if name_text and len(name_text) > 2:
                            establishment_name = name_text
                            break
                    except:
                        continue
                establishment_data["name"] = establishment_name
            except:
                establishment_data["name"] = "N/A"
            
            # Detect establishment type
            establishment_type = self.detect_establishment_type(establishment_url)
            establishment_data["establishment_type"] = establishment_type
            
            print(f"[*] Detected as {establishment_type}: {establishment_name}")
            
            # Scrape items based on type
            items = []
            items_error = None
            
            def scrape_items_thread():
                nonlocal items, items_error
                try:
                    if establishment_type == "store":
                        print(f"[*] Starting store carousel scraping for: {establishment_name}")
                        items = self.scrape_store_carousels(max_menu_items)
                    else:
                        print(f"[*] Starting restaurant menu scraping for: {establishment_name}")
                        items = scrape_menu_for_restaurant(self.driver, max_menu_items)
                    
                    print(f"[✓] {establishment_type.capitalize()} scraping completed: {len(items)} items")
                    
                except Exception as e:
                    items_error = str(e)
                    print(f"[!] {establishment_type.capitalize()} scraping failed: {e}")
            
            # Start items scraping in background
            if max_menu_items is None or max_menu_items > 0:
                items_thread = threading.Thread(target=scrape_items_thread)
                items_thread.start()
            
            # Get contact information
            info_link_found = False
            for attempt in range(3):
                try:
                    info_selectors = [
                        '//a[contains(text(), "Informations") or contains(text(), "informations")]',
                        '//a[contains(@href, "storeInfo") or contains(@href, "info")]',
                        'a[class*="af"][class*="d3"][class*="db"][class*="e4"][class*="ec"][class*="de"][class*="ee"]'
                    ]
                    
                    info_link = None
                    for selector in info_selectors:
                        try:
                            if selector.startswith('//'):
                                info_link = self.driver.find_element(By.XPATH, selector)
                            else:
                                info_link = self.driver.find_element(By.CSS_SELECTOR, selector)
                            if info_link:
                                break
                        except:
                            continue
                    
                    if info_link:
                        info_url = info_link.get_attribute("href")
                        if info_url:
                            self.driver.get(info_url)
                            time.sleep(2)
                            
                            self.extract_contact_info(establishment_data)
                            info_link_found = True
                            break
                        
                except Exception as e:
                    print(f"[!] Info link attempt {attempt + 1} failed: {e}")
                    time.sleep(1)
                    continue
            
            if not info_link_found:
                establishment_data.update({"email": "N/A", "phone": "N/A", "registration_number": "N/A"})
            
            # Wait for items scraping to complete
            if max_menu_items is None or max_menu_items > 0:
                items_thread.join(timeout=60)
                
                if items_thread.is_alive():
                    print(f"[!] Items scraping timeout")
                    items = []
                elif items_error:
                    print(f"[!] Items scraping error: {items_error}")
                    items = []
            
            # Add items to establishment data with proper structure
            if establishment_type == "store":
                establishment_data["products"] = items  
                establishment_data["products_count"] = len(items)
            else:
                establishment_data["menu_items"] = items  
                establishment_data["menu_items_count"] = len(items)
            
            print(f"[✓] {establishment_type.capitalize()} complete: {establishment_name} | Items: {len(items)}")
            
            return establishment_data
            
        except Exception as e:
            print(f"[!] Error in get_info: {e}")
            return None

    def extract_contact_info(self, establishment_data):
        """Extract contact information from info page"""
        try:
            # Scroll to load content
            for i in range(2):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
            
            spans = self.driver.find_elements(By.CSS_SELECTOR, 'div span')
            
            for span in spans:
                try:
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
                            establishment_data["email"] = email
                            establishment_data["phone"] = phone
                            establishment_data["registration_number"] = registration
                            print(f"[✓] Contact info extracted successfully")
                            return
                            
                except Exception:
                    continue
            
            # If no contact info found, set defaults
            establishment_data.setdefault("email", "N/A")
            establishment_data.setdefault("phone", "N/A")
            establishment_data.setdefault("registration_number", "N/A")
            
        except Exception as e:
            print(f"[!] Error extracting contact info: {e}")
            establishment_data.update({"email": "N/A", "phone": "N/A", "registration_number": "N/A"})

    def get_establishment(self, postal_code, max_restaurants=None, max_menu_items=None):
        """Scrape all establishments with enhanced store deduplication"""
        try:
            # Wait for cards to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-testid="store-card"]'))
            )
            
            cards = self.driver.find_elements(By.CSS_SELECTOR, 'a[data-testid="store-card"]')
            total_cards = len(cards)
            successful_count = 0
            
            print(f"[+] Found {total_cards} establishment cards")
            
            for index, card in enumerate(cards):
                try:
                    if max_restaurants and successful_count >= max_restaurants:
                        print(f"[+] Reached limit of {max_restaurants} establishments")
                        break
                    
                    establishment_url = card.get_attribute("href")
                    if not establishment_url:
                        continue
                    
                    # Check URL-based duplication (existing logic)
                    if establishment_url in self.scraped_urls:
                        print(f"[!] {index + 1}/{total_cards} - Already scraped URL, skipping")
                        continue
                    
                    # Get establishment name from card (for pre-checking)
                    try:
                        name_element = card.find_element(By.CSS_SELECTOR, 'h3, h2, [data-testid*="name"], [class*="name"]')
                        card_name = name_element.text.strip() if name_element else ""
                    except:
                        card_name = ""
                    
                    # Pre-check for store name duplication (saves time)
                    if card_name and self.is_store_already_scraped(card_name, "store"):
                        print(f"[!] {index + 1}/{total_cards} - Store name already scraped, skipping: {card_name}")
                        continue
                    
                    # Open in new tab
                    self.driver.execute_script("window.open(arguments[0]);", establishment_url)
                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    
                    # Process establishment
                    establishment_data = self.get_info(establishment_url, postal_code, max_menu_items)
                    if establishment_data:
                        establishment_name = establishment_data.get("name", "N/A")
                        establishment_type = establishment_data.get("establishment_type", "restaurant")
                        
                        # Final check for store name duplication after getting full data
                        if establishment_type == "store" and self.is_store_already_scraped(establishment_name, establishment_type):
                            print(f"[!] {index + 1}/{total_cards} - Duplicate store detected after scraping: {establishment_name}")
                            # Close tab and continue
                            self.driver.close()
                            self.driver.switch_to.window(self.driver.window_handles[0])
                            continue
                        
                        # Add to our collection
                        self.establishments_data.append(establishment_data)
                        successful_count += 1
                        self.scraped_urls.add(establishment_url)
                        
                        # Add store name to scraped names
                        if establishment_type == "store" and establishment_name != "N/A":
                            normalized_name = self.normalize_store_name(establishment_name)
                            self.scraped_store_names.add(normalized_name)
                            print(f"[+] Added store name to tracking: {establishment_name} -> {normalized_name}")
                        
                        item_count = establishment_data.get("products_count", 0) + establishment_data.get("menu_items_count", 0)
                        
                        print(f"[✓] {successful_count}/{max_restaurants if max_restaurants else '∞'} - {establishment_name} ({establishment_type}) | Items: {item_count}")
                    else:
                        print(f"[!] {index + 1}/{total_cards} - Failed to extract data")
                    
                    # Close tab and switch back
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])
                    
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"[!] Error on card {index + 1}: {e}")
                    try:
                        # Ensure we're back to main window
                        if len(self.driver.window_handles) > 1:
                            self.driver.close()
                        self.driver.switch_to.window(self.driver.window_handles[0])
                    except:
                        pass
                    continue
            
            print(f"[+] Page complete: {successful_count} new establishments scraped")
            return successful_count
            
        except Exception as e:
            print(f"[!] Error in get_establishment: {e}")
            return 0

    def check_existing_stores_before_scraping(postal_code):
        """Load existing store URLs to prevent duplicate scraping"""
        store_filename = f"stores_{postal_code}.json"
        existing_urls = set()
        
        if os.path.exists(store_filename):
            try:
                with open(store_filename, 'r', encoding='utf-8') as f:
                    stores_data = json.load(f)
                    for store in stores_data:
                        url = store.get('url', '')
                        if url:
                            existing_urls.add(url)
                    print(f"[+] Loaded {len(existing_urls)} existing store URLs")
            except Exception as e:
                print(f"[!] Error loading existing stores: {e}")
        
        return existing_urls

    def categorize_and_save_establishments(self, postal_code):
        """STEP 2: Categorize all scraped establishments and save to separate files - UPDATED"""
        try:
            restaurants = []
            stores = []
            
            print(f"\n[*] Categorizing {len(self.establishments_data)} establishments...")
            
            for establishment in self.establishments_data:
                establishment_type = establishment.get("establishment_type", "restaurant")
                
                # FIXED: Double-check categorization based on scraped items
                if establishment_type == "restaurant":
                    menu_items = establishment.get("menu_items", [])
                    if menu_items:
                        # Check if menu items look like store products
                        # If menu items have only descriptions (no titles), likely misclassified
                        items_with_titles = sum(1 for item in menu_items if item.get("title"))
                        items_with_descriptions = sum(1 for item in menu_items if item.get("description"))
                        
                        # If most items don't have titles, might be a store
                        if items_with_titles < (items_with_descriptions * 0.3):
                            detected_type = detect_establishment_type_from_items(menu_items)
                            if detected_type == "store":
                                print(f"[!] Re-categorizing {establishment.get('name', 'N/A')} from restaurant to store")
                                establishment["establishment_type"] = "store"
                                # Convert menu_items to products (descriptions only)
                                products = []
                                for item in menu_items:
                                    if item.get("description"):
                                        products.append({"description": item["description"]})
                                establishment["products"] = products
                                establishment["products_count"] = len(products)
                                establishment.pop("menu_items", None)
                                establishment.pop("menu_items_count", None)
                                establishment_type = "store"
                
                # Add to appropriate category
                if establishment_type == "store":
                    stores.append(establishment)
                else:
                    restaurants.append(establishment)
            
            # Save restaurants
            restaurant_filename = f"restaurants_{postal_code}.json"
            if restaurants:
                try:
                    with open(restaurant_filename, 'w', encoding='utf-8') as f:
                        json.dump(restaurants, f, indent=2, ensure_ascii=False)
                    print(f"[✓] Saved {len(restaurants)} restaurants to {restaurant_filename}")
                except Exception as e:
                    print(f"[!] Error saving restaurants: {e}")
            
            # Save stores
            store_filename = f"stores_{postal_code}.json"
            if stores:
                try:
                    with open(store_filename, 'w', encoding='utf-8') as f:
                        json.dump(stores, f, indent=2, ensure_ascii=False)
                    print(f"[✓] Saved {len(stores)} stores to {store_filename}")
                except Exception as e:
                    print(f"[!] Error saving stores: {e}")
            
            # NEW: Post-process files to remove duplicates and clean data
            post_process_scraped_files(postal_code)
            
            print(f"\n[✓] Categorization and cleaning complete:")
            print(f"   • Restaurants: {len(restaurants)}")
            print(f"   • Stores: {len(stores)}")
            print(f"   • Total: {len(restaurants) + len(stores)}")
            
            return {
                'restaurants': len(restaurants),
                'stores': len(stores),
                'total': len(restaurants) + len(stores)
            }
            
        except Exception as e:
            print(f"[!] Error in categorize_and_save_establishments: {e}")
            return {'restaurants': 0, 'stores': 0, 'total': 0}

    
    def scrape_page(self, postal_code, max_restaurants=None, max_menu_items=None):
        """Main scraping function - scrape all first, then categorize"""
        print(f"[*] Starting ENHANCED scraping for postal code: {postal_code}")
        if max_restaurants:
            print(f"[*] Maximum establishments to scrape: {max_restaurants}")
        if max_menu_items:
            print(f"[*] Maximum menu items per establishment: {max_menu_items}")
        
        # Clear previous data
        self.establishments_data = []
        self.load_existing_urls(postal_code)
        
        page_count = 1
        total_scraped = 0
        
        while True:
            print(f"\n[*] Processing page {page_count}")
            
            if max_restaurants and total_scraped >= max_restaurants:
                print(f"[+] Reached target of {max_restaurants} establishments")
                break
            
            # Enhanced scrolling
            try:
                last_height = self.driver.execute_script("return document.body.scrollHeight")
                scroll_attempts = 0
                max_scroll_attempts = 10
                
                while scroll_attempts < max_scroll_attempts:
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1.5)
                    
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
                    scroll_attempts += 1
                
            except Exception as e:
                print(f"[!] Scrolling error: {e}")
            
            remaining = max_restaurants - total_scraped if max_restaurants else None
            
            scraped_count = self.get_establishment(postal_code, remaining, max_menu_items)
            total_scraped += scraped_count
            
            print(f"[+] Page {page_count}: {scraped_count} new establishments | Total: {total_scraped}")
            
            if max_restaurants and total_scraped >= max_restaurants:
                print(f"[✓] Target reached: {total_scraped}/{max_restaurants} establishments")
                break
            
            # Enhanced "Show more" button handling
            try:
                show_more_selectors = [
                    'button.ky.br.bo.ds.dk.o5.e8.al.bc.d4.af.o6.o7.j1.o8.o9.oa.gr.gs.ob',
                    'button[data-testid="load-more"]'
                ]
                
                show_more_button = None
                for selector in show_more_selectors:
                    try:
                        show_more_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if show_more_button.is_displayed() and show_more_button.is_enabled():
                            break
                    except:
                        continue
                
                if show_more_button and show_more_button.is_displayed() and show_more_button.is_enabled():
                    print(f"[+] Clicking 'Show more' for page {page_count + 1}")
                    
                    if self.robust_click(show_more_button):
                        time.sleep(2)
                        page_count += 1
                    else:
                        print(f"[!] Failed to click 'Show more' button")
                        break
                else:
                    print(f"[+] No more pages available")
                    break
                    
            except Exception as e:
                print(f"[!] Error with 'Show more' button: {e}")
                break
        
        print(f"\n[*] Scraping complete. Starting categorization...")
        
        # Categorize and save all establishments
        categorization_results = self.categorize_and_save_establishments(postal_code)
        
        print(f"[✓] FINAL RESULTS:")
        print(f"   • Pages processed: {page_count}")
        print(f"   • Total establishments: {total_scraped}")
        print(f"   • Restaurants: {categorization_results['restaurants']}")
        print(f"   • Stores: {categorization_results['stores']}")
        
        return {
            'pages_processed': page_count,
            'establishments_scraped': total_scraped,
            'restaurants_scraped': categorization_results['restaurants'],
            'stores_scraped': categorization_results['stores']
        }

    def close_driver(self):
        """Close the browser driver"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass


def is_price_only_description(description):
    """FIXED: Check if description is only a price or quantity"""
    if not description or len(description.strip()) < 3:
        return True
    
    desc = description.strip()
    
    # Fixed patterns that match your actual data
    patterns_to_remove = [
        r'^\(\d+[,\.]\d+\s*€/kg\)$',          # (4,80 €/kg)
        r'^\(\d+[,\.]\d+\s*€/pièce\)$',       # (2,17 €/pièce)
        r'^\d+\s*pcs?\s*•\s*\d+[,\.]?\d*\s*g$',   # 9 pcs • 23.5 g
        r'^\d+\s*pcs?\s*•\s*\d+[,\.]?\d*\s*ml$',  # 6 pcs • 330 ml
        r'^\(\d+[,\.]\d+\s*€.*\)$',           # Any price in parentheses
    ]
    
    for pattern in patterns_to_remove:
        if re.match(pattern, desc, re.IGNORECASE):
            return True
    
    # Check if it's mostly numbers and symbols
    if re.match(r'^[\(\)\d\s€,./-]+$', desc):
        return True
    
    # Must have letters and reasonable length
    if len(desc) < 8 or not re.search(r'[a-zA-ZÀ-ÿ]', desc):
        return True
    
    return False

def clean_store_products(products):
    """Remove price-only and quantity-only descriptions from products"""
    cleaned_products = []
    removed_count = 0
    
    for product in products:
        description = product.get('description', '').strip()
        
        if description and not is_price_only_description(description):
            # Additional validation - must have letters and reasonable length
            if (len(description) > 8 and 
                re.search(r'[a-zA-ZÀ-ÿ]', description) and  # Contains letters
                len(description) < 300):  # Not too long
                cleaned_products.append(product)
            else:
                removed_count += 1
        else:
            removed_count += 1
    
    print(f"[+] Cleaned products: kept {len(cleaned_products)}, removed {removed_count}")
    return cleaned_products

def remove_duplicate_stores(stores_data):
    """Remove duplicate stores based on URL"""
    seen_urls = set()
    unique_stores = []
    duplicates_removed = 0
    
    for store in stores_data:
        url = store.get('url', '')
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_stores.append(store)
        else:
            duplicates_removed += 1
            print(f"[!] Removed duplicate store: {store.get('name', 'Unknown')} - {url}")
    
    print(f"[+] Duplicate removal: kept {len(unique_stores)}, removed {duplicates_removed}")
    return unique_stores

def clean_and_deduplicate_file(filename):
    """Clean and deduplicate a stores file"""
    if not os.path.exists(filename):
        print(f"[!] File not found: {filename}")
        return False
    
    try:
        print(f"[*] Processing file: {filename}")
        
        # Read existing data
        with open(filename, 'r', encoding='utf-8') as f:
            stores_data = json.load(f)
        
        original_count = len(stores_data)
        original_products = sum(len(store.get('products', [])) for store in stores_data)
        
        print(f"[*] Original: {original_count} stores, {original_products} products")
        
        # Step 1: Remove duplicate stores
        unique_stores = remove_duplicate_stores(stores_data)
        
        # Step 2: Clean products in each store
        total_cleaned_products = 0
        for store in unique_stores:
            original_product_count = len(store.get('products', []))
            cleaned_products = clean_store_products(store.get('products', []))
            store['products'] = cleaned_products
            store['products_count'] = len(cleaned_products)
            total_cleaned_products += len(cleaned_products)
            
            if original_product_count != len(cleaned_products):
                print(f"[+] {store.get('name', 'Unknown')}: {original_product_count} → {len(cleaned_products)} products")
        
        # Step 3: Save cleaned data
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(unique_stores, f, indent=2, ensure_ascii=False)
        
        print(f"[✓] Cleaning complete:")
        print(f"    • Stores: {original_count} → {len(unique_stores)}")
        print(f"    • Products: {original_products} → {total_cleaned_products}")
        print(f"    • File saved: {filename}")
        
        return True
        
    except Exception as e:
        print(f"[!] Error cleaning file {filename}: {e}")
        return False

def post_process_scraped_files(postal_code):
    """Post-process both restaurant and store files after scraping"""
    print(f"\n[*] Starting post-processing for postal code: {postal_code}")
    
    restaurant_filename = f"restaurants_{postal_code}.json"
    store_filename = f"stores_{postal_code}.json"
    
    # Clean restaurants file (just remove duplicates, keep menu items as-is)
    if os.path.exists(restaurant_filename):
        try:
            with open(restaurant_filename, 'r', encoding='utf-8') as f:
                restaurants_data = json.load(f)
            
            original_count = len(restaurants_data)
            
            # Remove duplicate restaurants by URL
            seen_urls = set()
            unique_restaurants = []
            
            for restaurant in restaurants_data:
                url = restaurant.get('url', '')
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    unique_restaurants.append(restaurant)
            
            if len(unique_restaurants) != original_count:
                with open(restaurant_filename, 'w', encoding='utf-8') as f:
                    json.dump(unique_restaurants, f, indent=2, ensure_ascii=False)
                print(f"[+] Restaurants: {original_count} → {len(unique_restaurants)} (removed duplicates)")
            else:
                print(f"[+] Restaurants: {len(unique_restaurants)} (no duplicates found)")
                
        except Exception as e:
            print(f"[!] Error processing restaurants file: {e}")
    
    # Clean stores file (remove duplicates AND clean products)
    if os.path.exists(store_filename):
        clean_and_deduplicate_file(store_filename)
    
    print(f"[✓] Post-processing complete for postal code: {postal_code}")

def clean_existing_files(postal_code):
    """Utility function to clean existing files manually"""
    print(f"\n[*] Manual cleaning initiated for postal code: {postal_code}")
    
    restaurant_filename = f"restaurants_{postal_code}.json"
    store_filename = f"stores_{postal_code}.json"
    
    files_processed = 0
    
    # Check and clean restaurants file
    if os.path.exists(restaurant_filename):
        print(f"[*] Found restaurants file: {restaurant_filename}")
        try:
            with open(restaurant_filename, 'r', encoding='utf-8') as f:
                restaurants_data = json.load(f)
            
            original_count = len(restaurants_data)
            
            # Remove duplicates by URL
            seen_urls = set()
            unique_restaurants = []
            
            for restaurant in restaurants_data:
                url = restaurant.get('url', '')
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    unique_restaurants.append(restaurant)
            
            # Save cleaned restaurants
            with open(restaurant_filename, 'w', encoding='utf-8') as f:
                json.dump(unique_restaurants, f, indent=2, ensure_ascii=False)
            
            print(f"[✓] Restaurants cleaned: {original_count} → {len(unique_restaurants)}")
            files_processed += 1
            
        except Exception as e:
            print(f"[!] Error cleaning restaurants file: {e}")
    else:
        print(f"[!] Restaurants file not found: {restaurant_filename}")
    
    # Check and clean stores file
    if os.path.exists(store_filename):
        print(f"[*] Found stores file: {store_filename}")
        if clean_and_deduplicate_file(store_filename):
            files_processed += 1
    else:
        print(f"[!] Stores file not found: {store_filename}")
    
    if files_processed > 0:
        print(f"\n[✓] Manual cleaning complete: {files_processed} files processed")
        
        # Show updated stats
        stats = get_categorization_stats(postal_code)
        if stats:
            print(f"\n[+] Updated Statistics:")
            print(f"   • Restaurants: {stats['restaurants']['count']}")
            print(f"   • Stores: {stats['stores']['count']}")
            print(f"   • Total Items: {stats['totals']['total_items']}")
    else:
        print(f"\n[!] No files were processed")



def get_categorization_stats(postal_code):
    """Get comprehensive statistics for categorized establishments"""
    try:
        restaurant_filename = f"restaurants_{postal_code}.json"
        store_filename = f"stores_{postal_code}.json"
        
        # Initialize stats
        stats = {
            'restaurants': {
                'count': 0,
                'total_menu_items': 0,
                'avg_menu_items': 0,
                'sample_names': []
            },
            'stores': {
                'count': 0,
                'total_products': 0,
                'avg_products': 0,
                'sample_names': []
            },
            'totals': {
                'establishments': 0,
                'total_items': 0,
                'menu_items': 0,
                'products': 0
            }
        }
        
        # Process restaurants
        if os.path.exists(restaurant_filename):
            try:
                with open(restaurant_filename, 'r', encoding='utf-8') as f:
                    restaurant_data = json.load(f)
                    
                    stats['restaurants']['count'] = len(restaurant_data)
                    stats['restaurants']['total_menu_items'] = sum(
                        item.get('menu_items_count', 0) for item in restaurant_data
                    )
                    stats['restaurants']['avg_menu_items'] = round(
                        stats['restaurants']['total_menu_items'] / max(1, stats['restaurants']['count']), 1
                    )
                    stats['restaurants']['sample_names'] = [
                        item.get('name', 'N/A') for item in restaurant_data[:5] 
                        if item.get('name') != 'N/A'
                    ]
                    
            except Exception as e:
                print(f"[!] Error reading restaurants file: {e}")
        
        # Process stores
        if os.path.exists(store_filename):
            try:
                with open(store_filename, 'r', encoding='utf-8') as f:
                    store_data = json.load(f)
                    
                    stats['stores']['count'] = len(store_data)
                    stats['stores']['total_products'] = sum(
                        item.get('products_count', 0) for item in store_data
                    )
                    stats['stores']['avg_products'] = round(
                        stats['stores']['total_products'] / max(1, stats['stores']['count']), 1
                    )
                    stats['stores']['sample_names'] = [
                        item.get('name', 'N/A') for item in store_data[:5] 
                        if item.get('name') != 'N/A'
                    ]
                    
            except Exception as e:
                print(f"[!] Error reading stores file: {e}")
        
        # Calculate totals
        stats['totals']['establishments'] = stats['restaurants']['count'] + stats['stores']['count']
        stats['totals']['menu_items'] = stats['restaurants']['total_menu_items']
        stats['totals']['products'] = stats['stores']['total_products']
        stats['totals']['total_items'] = stats['totals']['menu_items'] + stats['totals']['products']
        
        return stats
        
    except Exception as e:
        print(f"[!] Error calculating categorization stats: {e}")
        return None


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
    """Perform search and full establishment scraping with ENHANCED error handling"""
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
                'error': 'Failed to search postal code - possible overlay or click interception issue'
            }
        
        time.sleep(5)
        
        scraping_start = time.time()
        scrape_results = scraper.scrape_page(postal_code, max_restaurants, max_menu_items)
        scraping_time = time.time() - scraping_start
        
        total_time = time.time() - start_time
        
        # Get categorization stats
        stats = get_categorization_stats(postal_code)
        
        return {
            'success': True,
            'postal_code': postal_code,
            'scraping_results': scrape_results,
            'categorization_stats': stats,
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
            'message': f"Scraped {scrape_results['establishments_scraped']} establishments ({scrape_results['restaurants_scraped']} restaurants, {scrape_results['stores_scraped']} stores) from {scrape_results['pages_processed']} pages",
            'output_files': {
                'restaurants': f"restaurants_{postal_code}.json",
                'stores': f"stores_{postal_code}.json"
            }
        }
    
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }
    
    finally:
        scraper.close_driver()


def standalone_scrape(postal_code, visible=False, max_restaurants=None, max_menu_items=None):
    """Run ENHANCED scraping directly without Flask API"""
    
    print("\n##########################################################")
    print("########### ENHANCED UBEREATS SCRAPER ###############")
    print("##########################################################\n")
    
    print(f"Starting ENHANCED standalone scraping...")
    print(f"Postal Code: {postal_code}")
    print(f"Visible Mode: {'ON' if visible else 'OFF'}")
    if max_restaurants:
        print(f"Max Establishments: {max_restaurants}")
    if max_menu_items:
        print(f"Max Menu Items: {max_menu_items}")
    
    print(f"\n[*] ENHANCED Features:")
    print(f"   • Multi-method click handling (Standard + JavaScript + ActionChains + Force)")
    print(f"   • Advanced overlay dismissal (including data-baseweb elements)")
    print(f"   • Enhanced error recovery and retry logic")
    print(f"   • Improved page loading detection")
    print(f"   • Better element visibility and clickability checks")
    print(f"   • Conservative restaurant/store detection")
    print(f"   • Store products: descriptions only (simplified validation)")
    print(f"   • Restaurant menus: title + description")
    
    # Run the scraping
    result = perform_full_scrape(postal_code, visible, max_restaurants, max_menu_items)
    
    if result.get('success'):
        print(f"\n[✓] ENHANCED scraping completed successfully!")
        print(f"[+] {result['message']}")
        
        # Show output files
        output_files = result.get('output_files', {})
        print(f"\n[+] Output files:")
        for file_type, filename in output_files.items():
            if os.path.exists(filename):
                file_size = os.path.getsize(filename)
                print(f"   • {file_type.capitalize()}: {filename} ({round(file_size/1024, 1)} KB)")
            else:
                print(f"   • {file_type.capitalize()}: {filename} (not created)")
        
        # Show categorization stats
        stats = result.get('categorization_stats')
        if stats:
            print(f"\n[>] ENHANCED Categorization Results:")
            print(f"   • Restaurants: {stats['restaurants']['count']} (avg {stats['restaurants']['avg_menu_items']} menu items)")
            print(f"   • Stores: {stats['stores']['count']} (avg {stats['stores']['avg_products']} products)")
            print(f"   • Total Establishments: {stats['totals']['establishments']}")
            print(f"   • Total Items: {stats['totals']['total_items']}")
            
            if stats['restaurants']['sample_names']:
                print(f"   • Sample restaurants: {', '.join(stats['restaurants']['sample_names'][:3])}")
            if stats['stores']['sample_names']:
                print(f"   • Sample stores: {', '.join(stats['stores']['sample_names'][:3])}")
        
        scraping_results = result['scraping_results']
        timing = result['timing']
        print(f"\n[>] Performance:")
        print(f"   • Total Time: {timing['total_time']} seconds")
        print(f"   • Scraping Time: {timing['scraping_time']} seconds")
        print(f"   • Pages Processed: {scraping_results['pages_processed']}")
        
        return True
    else:
        print(f"\n[!] ENHANCED scraping failed!")
        print(f"[!] Error: {result.get('error', 'Unknown error')}")
        return False


def check_api_health(base_url="http://localhost:5005"):
    """Check if the Flask API is running"""
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        if response.status_code == 200:
            return True
    except:
        pass
    return False


def analyze_postal_code_data(postal_code):
    """Analyze data quality for a postal code"""
    try:
        stats = get_categorization_stats(postal_code)
        if not stats:
            return None
        
        analysis = {
            'postal_code': postal_code,
            'data_quality': {
                'total_establishments': stats['totals']['establishments'],
                'restaurant_ratio': round(stats['restaurants']['count'] / max(1, stats['totals']['establishments']), 2),
                'store_ratio': round(stats['stores']['count'] / max(1, stats['totals']['establishments']), 2),
                'avg_items_per_establishment': round(stats['totals']['total_items'] / max(1, stats['totals']['establishments']), 1)
            },
            'restaurant_analysis': {
                'count': stats['restaurants']['count'],
                'total_menu_items': stats['restaurants']['total_menu_items'],
                'avg_menu_items': stats['restaurants']['avg_menu_items'],
                'quality_score': 'High' if stats['restaurants']['avg_menu_items'] > 15 else 'Medium' if stats['restaurants']['avg_menu_items'] > 8 else 'Low'
            },
            'store_analysis': {
                'count': stats['stores']['count'],
                'total_products': stats['stores']['total_products'],
                'avg_products': stats['stores']['avg_products'],
                'quality_score': 'High' if stats['stores']['avg_products'] > 25 else 'Medium' if stats['stores']['avg_products'] > 12 else 'Low'
            },
            'recommendations': []
        }
        
        # Add recommendations
        if stats['restaurants']['avg_menu_items'] < 8:
            analysis['recommendations'].append("Consider increasing menu scraping depth for restaurants")
        
        if stats['stores']['avg_products'] < 12:
            analysis['recommendations'].append("Consider improving carousel navigation for stores")
        
        if stats['totals']['establishments'] < 20:
            analysis['recommendations'].append("Consider increasing establishment limit for more comprehensive data")
        
        return analysis
        
    except Exception as e:
        print(f"[!] Error analyzing postal code data: {e}")
        return None


def main():
    """Command line interface for standalone scraping"""
    parser = argparse.ArgumentParser(description='UberEats Scraper - Standalone Mode')
    parser.add_argument('--postal', required=True, help='Postal code to search')
    parser.add_argument('--visible', action='store_true', help='Run browser in visible mode')
    parser.add_argument('--limit', type=int, help='Maximum number of establishments to scrape')
    parser.add_argument('--menu-limit', type=int, help='Maximum number of menu items per establishment')
    parser.add_argument('--analyze', action='store_true', help='Analyze existing data for postal code')
    parser.add_argument('--clean', action='store_true', help='Clean existing files for postal code')  # NEW
    
    args = parser.parse_args()
    
    # Handle cleaning requests - ADD THIS SECTION
    if args.clean:
        clean_existing_files(args.postal)
        return
    
    # Handle analysis requests
    if args.analyze:
        analysis = analyze_postal_code_data(args.postal)
        if analysis:
            print(f"\n[*] Analysis for postal code: {args.postal}")
            print(f"[*] Total establishments: {analysis['data_quality']['total_establishments']}")
            print(f"[*] Restaurant ratio: {analysis['data_quality']['restaurant_ratio']}")
            print(f"[*] Store ratio: {analysis['data_quality']['store_ratio']}")
            print(f"[*] Avg items per establishment: {analysis['data_quality']['avg_items_per_establishment']}")
            
            if analysis['recommendations']:
                print(f"\n[*] Recommendations:")
                for rec in analysis['recommendations']:
                    print(f"   • {rec}")
        else:
            print(f"[!] No data found for postal code: {args.postal}")
        return
    
    # Run scraping (rest of the function remains the same...)
    success = standalone_scrape(
        args.postal, 
        args.visible, 
        args.limit, 
        args.menu_limit
    )
    
    if success:
        print(f"\n[+] Script completed successfully!")
        
        # Show final stats
        stats = get_categorization_stats(args.postal)
        if stats:
            print(f"\n[+] Final Statistics:")
            print(f"   • Total establishments: {stats['totals']['establishments']}")
            print(f"   • Restaurants: {stats['restaurants']['count']}")
            print(f"   • Stores: {stats['stores']['count']}")
            print(f"   • Total items: {stats['totals']['total_items']}")
    else:
        print(f"\n[!] Script failed!")
        sys.exit(1)     


if __name__ == "__main__":
    main()
# === main.py CONTENT END ===

# === app.py CONTENT START ===
from flask import Flask, request, jsonify
from main import perform_search, perform_full_scrape, get_categorization_stats, analyze_postal_code_data
import os
import json
import threading
import time
from datetime import datetime
import uuid

app = Flask(__name__)

# Global storage for active scraping jobs
active_jobs = {}
job_status = {}

class ScrapingJob:
    def __init__(self, job_id, postal_code, visible, max_restaurants, max_menu_items):
        self.job_id = job_id
        self.postal_code = postal_code
        self.visible = visible
        self.max_restaurants = max_restaurants
        self.max_menu_items = max_menu_items
        self.status = "starting"
        self.start_time = datetime.now()
        self.progress = {
            "establishments_scraped": 0, 
            "current_establishment": "", 
            "current_type": "",
            "restaurants_found": 0,
            "stores_found": 0,
            "total_menu_items": 0,
            "total_products": 0,
            "carousels_processed": 0,
            "duplicates_prevented": 0,  # NEW
            "store_duplicates_prevented": 0,  # NEW
            "error": None
        }
        self.result = None
        
    def to_dict(self):
        return {
            "job_id": self.job_id,
            "postal_code": self.postal_code,
            "status": self.status,
            "start_time": self.start_time.isoformat(),
            "progress": self.progress,
            "result": self.result
        }

def run_scraping_job(job):
    """Run scraping in background thread with enhanced monitoring and deduplication"""
    try:
        job.status = "running"
        active_jobs[job.job_id] = job
        
        print(f"[*] Starting enhanced background scraping job {job.job_id}")
        print(f"[*] Features: Store carousel navigation, Store deduplication, Simplified validation, Dual file output")
        
        # Run the actual scraping
        result = perform_full_scrape(
            job.postal_code, 
            job.visible, 
            job.max_restaurants, 
            job.max_menu_items
        )
        
        job.result = result
        job.status = "completed" if result.get('success') else "failed"
        job.progress["error"] = result.get('error') if not result.get('success') else None
        
        # Update final progress with categorization stats
        if result.get('success'):
            stats = get_categorization_stats(job.postal_code)
            if stats:
                job.progress.update({
                    "restaurants_found": stats['restaurants']['count'],
                    "stores_found": stats['stores']['count'],
                    "total_menu_items": stats['restaurants']['total_menu_items'],
                    "total_products": stats['stores']['total_products']
                })
                
            # Add deduplication stats from scraping results
            scraping_results = result.get('scraping_results', {})
            job.progress.update({
                "duplicates_prevented": scraping_results.get('duplicates_skipped', 0),
                "store_duplicates_prevented": scraping_results.get('store_duplicates_skipped', 0)
            })
        
        print(f"[*] Enhanced background job {job.job_id} completed with status: {job.status}")
        
    except Exception as e:
        job.status = "failed"
        job.progress["error"] = str(e)
        print(f"[!] Background job {job.job_id} failed: {e}")
    
    finally:
        # Keep job info for 2 hours after completion (increased for analysis)
        time.sleep(7200)
        if job.job_id in active_jobs:
            del active_jobs[job.job_id]

@app.route('/search', methods=['POST'])
def search_postal_code():
    """Search for postal code only"""
    try:
        data = request.get_json()
        postal_code = data.get('postal_code')
        visible = data.get('visible', False)

        if not postal_code:
            return jsonify({
                'success': False,
                'error': 'postal_code is required'
            }), 400

        # Call function from scrape.py
        result = perform_search(postal_code, visible)
        
        if result.get('success', False):
            return jsonify(result)
        else:
            return jsonify(result), 500

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/scrape', methods=['POST'])
def scrape_establishments():
    """Start enhanced scraping with store deduplication in background and return job ID"""
    try:
        data = request.get_json()
        postal_code = data.get('postal_code')
        visible = data.get('visible', False)
        max_restaurants = data.get('max_restaurants', None)  # Now means max establishments
        max_menu_items = data.get('max_menu_items', None)

        if not postal_code:
            return jsonify({
                'success': False,
                'error': 'postal_code is required'
            }), 400

        # Validate parameters
        if max_restaurants is not None:
            if not isinstance(max_restaurants, int) or max_restaurants <= 0:
                return jsonify({
                    'success': False,
                    'error': 'max_restaurants must be a positive integer'
                }), 400

        if max_menu_items is not None:
            if not isinstance(max_menu_items, int) or max_menu_items <= 0:
                return jsonify({
                    'success': False,
                    'error': 'max_menu_items must be a positive integer'
                }), 400

        # Check for existing data and provide deduplication info
        existing_stats = get_categorization_stats(postal_code)
        deduplication_info = ""
        if existing_stats and existing_stats['totals']['establishments'] > 0:
            deduplication_info = f" (Will skip {existing_stats['totals']['establishments']} existing establishments)"

        # Create enhanced background job
        job_id = str(uuid.uuid4())[:8]  # Short job ID
        job = ScrapingJob(job_id, postal_code, visible, max_restaurants, max_menu_items)
        
        # Start background thread
        thread = threading.Thread(target=run_scraping_job, args=(job,))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'job_id': job_id,
            'message': f'Enhanced scraping started for postal code {postal_code}{deduplication_info}',
            'status_url': f'/job/{job_id}',
            'estimated_time': 'This may take 1-4 hours depending on establishments and store carousels',
            'features': [
                'Store carousel navigation',
                'Store name deduplication (Franprix, Carrefour variants)',
                'URL-based duplicate prevention',
                'Simplified product validation', 
                'Dual JSON file output',
                'Real-time progress tracking',
                'Automatic price/quantity filtering'
            ],
            'deduplication': {
                'url_based': 'Prevents same URL from being scraped twice',
                'store_name_based': 'Normalizes store names (e.g., "Franprix Sprint" → "franprix")',
                'existing_data_loaded': existing_stats['totals']['establishments'] if existing_stats else 0
            },
            'note': 'Results saved to restaurants_{postal_code}.json and stores_{postal_code}.json',
            'monitoring': {
                'job_status': f'/job/{job_id}',
                'live_stats': f'/status/{postal_code}',
                'detailed_stats': f'/stats/{postal_code}',
                'data_analysis': f'/analyze/{postal_code}'
            }
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/job/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Get enhanced status of a scraping job with deduplication metrics"""
    try:
        if job_id not in active_jobs:
            return jsonify({
                'success': False,
                'error': f'Job {job_id} not found or expired'
            }), 404
        
        job = active_jobs[job_id]
        
        # Check both restaurants and stores files for real-time updates
        restaurant_filename = f"restaurants_{job.postal_code}.json"
        store_filename = f"stores_{job.postal_code}.json"
        
        restaurant_count = 0
        store_count = 0
        total_menu_items = 0
        total_products = 0
        latest_restaurants = []
        latest_stores = []
        store_name_variety = []
        
        # Count restaurants
        if os.path.exists(restaurant_filename):
            try:
                with open(restaurant_filename, 'r', encoding='utf-8') as f:
                    restaurant_data = json.load(f)
                    restaurant_count = len(restaurant_data)
                    total_menu_items = sum(item.get('menu_items_count', 0) for item in restaurant_data)
                    # Get latest 2 restaurants
                    latest_restaurants = [
                        {
                            'name': item.get('name', 'N/A'),
                            'menu_items_count': item.get('menu_items_count', 0)
                        }
                        for item in restaurant_data[-2:] if item.get('name') != 'N/A'
                    ]
            except:
                pass
        
        # Count stores with enhanced analysis
        if os.path.exists(store_filename):
            try:
                with open(store_filename, 'r', encoding='utf-8') as f:
                    store_data = json.load(f)
                    store_count = len(store_data)
                    total_products = sum(item.get('products_count', 0) for item in store_data)
                    
                    # Get latest 2 stores
                    latest_stores = [
                        {
                            'name': item.get('name', 'N/A'),
                            'products_count': item.get('products_count', 0)
                        }
                        for item in store_data[-2:] if item.get('name') != 'N/A'
                    ]
                    
                    # Analyze store name variety (check deduplication effectiveness)
                    store_names = [item.get('name', 'N/A') for item in store_data]
                    unique_store_brands = set()
                    for name in store_names:
                        if name != 'N/A':
                            name_lower = name.lower()
                            if 'franprix' in name_lower:
                                unique_store_brands.add('Franprix')
                            elif 'carrefour' in name_lower:
                                unique_store_brands.add('Carrefour')
                            elif 'monoprix' in name_lower:
                                unique_store_brands.add('Monoprix')
                            elif 'casino' in name_lower:
                                unique_store_brands.add('Casino')
                            else:
                                unique_store_brands.add(name.split()[0])
                    
                    store_name_variety = list(unique_store_brands)[:5]  # Top 5 brands
                    
            except:
                pass
        
        response = job.to_dict()
        response.update({
            'current_restaurants_saved': restaurant_count,
            'current_stores_saved': store_count,
            'total_establishments_saved': restaurant_count + store_count,
            'current_menu_items_saved': total_menu_items,
            'current_products_saved': total_products,
            'latest_restaurants': latest_restaurants,
            'latest_stores': latest_stores,
            'store_brand_variety': store_name_variety,  # NEW
            'deduplication_effectiveness': {  # NEW
                'unique_store_brands': len(store_name_variety),
                'duplicates_prevented': job.progress.get('duplicates_prevented', 0),
                'store_duplicates_prevented': job.progress.get('store_duplicates_prevented', 0)
            },
            'output_files': {
                'restaurants': restaurant_filename,
                'stores': store_filename
            },
            'runtime_minutes': int((datetime.now() - job.start_time).total_seconds() / 60),
            'performance': {
                'establishments_per_minute': round((restaurant_count + store_count) / max(1, (datetime.now() - job.start_time).total_seconds() / 60), 2),
                'avg_menu_items_per_restaurant': round(total_menu_items / max(1, restaurant_count), 1),
                'avg_products_per_store': round(total_products / max(1, store_count), 1)
            }
        })
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/jobs', methods=['GET'])
def list_active_jobs():
    """List all active jobs with enhanced deduplication info"""
    try:
        jobs_list = []
        for job_id, job in active_jobs.items():
            # Get current stats for each job
            stats = get_categorization_stats(job.postal_code)
            
            job_info = {
                'job_id': job_id,
                'postal_code': job.postal_code,
                'status': job.status,
                'start_time': job.start_time.isoformat(),
                'runtime_minutes': int((datetime.now() - job.start_time).total_seconds() / 60),
                'limits': {
                    'max_restaurants': job.max_restaurants,
                    'max_menu_items': job.max_menu_items
                },
                'deduplication_progress': {  # NEW
                    'duplicates_prevented': job.progress.get('duplicates_prevented', 0),
                    'store_duplicates_prevented': job.progress.get('store_duplicates_prevented', 0)
                }
            }
            
            if stats:
                job_info.update({
                    'current_progress': {
                        'restaurants': stats['restaurants']['count'],
                        'stores': stats['stores']['count'],
                        'total_establishments': stats['totals']['establishments'],
                        'total_items': stats['totals']['total_items']
                    }
                })
            
            jobs_list.append(job_info)
        
        return jsonify({
            'success': True,
            'active_jobs': jobs_list,
            'total_jobs': len(jobs_list),
            'system_info': {
                'uptime_minutes': int((datetime.now() - app_start_time).total_seconds() / 60),
                'features_enabled': [
                    'store_carousel_navigation',
                    'store_name_deduplication',  # NEW
                    'url_based_deduplication',   # NEW
                    'simplified_validation',
                    'dual_file_output',
                    'background_processing'
                ]
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/stop/<job_id>', methods=['POST'])
def stop_job(job_id):
    """Stop a running job (enhanced implementation with deduplication stats)"""
    try:
        if job_id not in active_jobs:
            return jsonify({
                'success': False,
                'error': f'Job {job_id} not found'
            }), 404
        
        job = active_jobs[job_id]
        old_status = job.status
        job.status = "stopped"
        
        # Get current progress before stopping
        stats = get_categorization_stats(job.postal_code)
        
        response = {
            'success': True,
            'message': f'Job {job_id} marked for stopping',
            'previous_status': old_status,
            'runtime_minutes': int((datetime.now() - job.start_time).total_seconds() / 60),
            'deduplication_summary': {  # NEW
                'duplicates_prevented': job.progress.get('duplicates_prevented', 0),
                'store_duplicates_prevented': job.progress.get('store_duplicates_prevented', 0)
            }
        }
        
        if stats:
            response['final_progress'] = {
                'restaurants_scraped': stats['restaurants']['count'],
                'stores_scraped': stats['stores']['count'],
                'total_establishments': stats['totals']['establishments'],
                'total_items': stats['totals']['total_items']
            }
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/status/<postal_code>', methods=['GET'])
def get_scraping_status(postal_code):
    """Get enhanced scraping status with deduplication analysis"""
    try:
        restaurant_filename = f"restaurants_{postal_code}.json"
        store_filename = f"stores_{postal_code}.json"
        
        # Initialize counts
        restaurant_count = 0
        store_count = 0
        total_menu_items = 0
        total_products = 0
        sample_restaurants = []
        sample_stores = []
        restaurant_file_size = 0
        store_file_size = 0
        store_brand_analysis = {}
        
        # Check restaurants file
        restaurant_exists = False
        if os.path.exists(restaurant_filename):
            restaurant_exists = True
            restaurant_file_size = os.path.getsize(restaurant_filename)
            try:
                with open(restaurant_filename, 'r', encoding='utf-8') as f:
                    restaurant_data = json.load(f)
                    restaurant_count = len(restaurant_data)
                    total_menu_items = sum(item.get('menu_items_count', 0) for item in restaurant_data)
                    sample_restaurants = [
                        {
                            'name': item.get('name', 'N/A'),
                            'menu_items_count': item.get('menu_items_count', 0),
                            'establishment_type': item.get('establishment_type', 'N/A')
                        }
                        for item in restaurant_data[:3] if item.get('name') != 'N/A'
                    ]
            except:
                pass
        
        # Check stores file with brand analysis
        store_exists = False
        if os.path.exists(store_filename):
            store_exists = True
            store_file_size = os.path.getsize(store_filename)
            try:
                with open(store_filename, 'r', encoding='utf-8') as f:
                    store_data = json.load(f)
                    store_count = len(store_data)
                    total_products = sum(item.get('products_count', 0) for item in store_data)
                    sample_stores = [
                        {
                            'name': item.get('name', 'N/A'),
                            'products_count': item.get('products_count', 0),
                            'establishment_type': item.get('establishment_type', 'N/A')
                        }
                        for item in store_data[:3] if item.get('name') != 'N/A'
                    ]
                    
                    # Analyze store brands (check deduplication effectiveness)
                    brand_counts = {}
                    for item in store_data:
                        name = item.get('name', 'N/A')
                        if name != 'N/A':
                            name_lower = name.lower()
                            if 'franprix' in name_lower:
                                brand_counts['Franprix'] = brand_counts.get('Franprix', 0) + 1
                            elif 'carrefour' in name_lower:
                                brand_counts['Carrefour'] = brand_counts.get('Carrefour', 0) + 1
                            elif 'monoprix' in name_lower:
                                brand_counts['Monoprix'] = brand_counts.get('Monoprix', 0) + 1
                            elif 'casino' in name_lower:
                                brand_counts['Casino'] = brand_counts.get('Casino', 0) + 1
                            else:
                                brand_name = name.split()[0]
                                brand_counts[brand_name] = brand_counts.get(brand_name, 0) + 1
                    
                    store_brand_analysis = dict(sorted(brand_counts.items(), key=lambda x: x[1], reverse=True)[:5])
                    
            except:
                pass
        
        # Check if there's an active job for this postal code
        active_job_info = None
        for job_id, job in active_jobs.items():
            if job.postal_code == postal_code:
                active_job_info = {
                    'job_id': job_id,
                    'status': job.status,
                    'runtime_minutes': int((datetime.now() - job.start_time).total_seconds() / 60),
                    'limits': {
                        'max_restaurants': job.max_restaurants,
                        'max_menu_items': job.max_menu_items
                    },
                    'progress': job.progress,
                    'deduplication_progress': {
                        'duplicates_prevented': job.progress.get('duplicates_prevented', 0),
                        'store_duplicates_prevented': job.progress.get('store_duplicates_prevented', 0)
                    }
                }
                break
        
        return jsonify({
            'success': True,
            'postal_code': postal_code,
            'restaurants': {
                'count': restaurant_count,
                'total_menu_items': total_menu_items,
                'avg_menu_items': round(total_menu_items / max(1, restaurant_count), 1),
                'samples': sample_restaurants,
                'file_exists': restaurant_exists,
                'filename': restaurant_filename,
                'file_size_kb': round(restaurant_file_size / 1024, 1) if restaurant_exists else 0
            },
            'stores': {
                'count': store_count,
                'total_products': total_products,
                'avg_products': round(total_products / max(1, store_count), 1),
                'samples': sample_stores,
                'file_exists': store_exists,
                'filename': store_filename,
                'file_size_kb': round(store_file_size / 1024, 1) if store_exists else 0,
                'brand_analysis': store_brand_analysis  # NEW
            },
            'totals': {
                'establishments': restaurant_count + store_count,
                'menu_items': total_menu_items,
                'products': total_products,
                'total_items': total_menu_items + total_products,
                'total_file_size_kb': round((restaurant_file_size + store_file_size) / 1024, 1)
            },
            'active_job': active_job_info,
            'detection_accuracy': {
                'restaurants_detected': restaurant_count,
                'stores_detected': store_count,
                'total_categorized': restaurant_count + store_count,
                'unique_store_brands': len(store_brand_analysis)  # NEW
            },
            'deduplication_effectiveness': {  # NEW section
                'unique_store_brands_found': len(store_brand_analysis),
                'store_brand_distribution': store_brand_analysis,
                'deduplication_working': len(store_brand_analysis) > 0 and max(store_brand_analysis.values()) == 1
            }
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/analyze/<postal_code>', methods=['GET'])
def analyze_postal_code_endpoint(postal_code):
    """NEW: Analyze data quality for a postal code using the scrape.py function"""
    try:
        analysis = analyze_postal_code_data(postal_code)
        
        if analysis:
            return jsonify({
                'success': True,
                'analysis': analysis
            })
        else:
            return jsonify({
                'success': False,
                'error': f'No data found for postal code {postal_code}'
            }), 404
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/clean/<postal_code>', methods=['POST'])
def clean_postal_code_files(postal_code):
    """Clean existing files with enhanced store deduplication"""
    try:
        if not postal_code:
            return jsonify({
                'success': False,
                'error': 'postal_code is required'
            }), 400
        
        print(f"[*] API cleaning request for postal code: {postal_code}")
        
        # Check if files exist before cleaning
        restaurant_filename = f"restaurants_{postal_code}.json"
        store_filename = f"stores_{postal_code}.json"
        
        files_found = []
        if os.path.exists(restaurant_filename):
            files_found.append('restaurants')
        if os.path.exists(store_filename):
            files_found.append('stores')
        
        if not files_found:
            return jsonify({
                'success': False,
                'error': f'No data files found for postal code {postal_code}',
                'files_checked': [restaurant_filename, store_filename]
            }), 404
        
        # Get stats before cleaning
        stats_before = get_categorization_stats(postal_code)
        
        # Import and run the cleaning script we created
        from artifacts import clean_store_duplicates  # Import our cleaning function
        cleaned_stores = clean_store_duplicates(postal_code)
        
        # Get stats after cleaning
        stats_after = get_categorization_stats(postal_code)
        
        # Calculate improvements
        improvements = {}
        if stats_before and stats_after:
            improvements = {
                'restaurants': {
                    'before': stats_before['restaurants']['count'],
                    'after': stats_after['restaurants']['count'],
                    'removed': stats_before['restaurants']['count'] - stats_after['restaurants']['count']
                },
                'stores': {
                    'before': stats_before['stores']['count'],
                    'after': stats_after['stores']['count'],
                    'removed': stats_before['stores']['count'] - stats_after['stores']['count']
                },
                'products': {
                    'before': stats_before['stores']['total_products'],
                    'after': stats_after['stores']['total_products'],
                    'removed': stats_before['stores']['total_products'] - stats_after['stores']['total_products']
                }
            }
        
        return jsonify({
            'success': True,
            'postal_code': postal_code,
            'message': f'Files cleaned successfully for postal code {postal_code}',
            'files_processed': files_found,
            'improvements': improvements,
            'current_stats': stats_after,
            'cleaning_features': [
                'duplicate_store_removal_by_name',  # UPDATED
                'price_only_description_removal',
                'quantity_only_description_removal',
                'invalid_product_filtering',
                'store_name_normalization'  # NEW
            ]
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/stats/<postal_code>', methods=['GET'])
def get_detailed_stats(postal_code):
    """Get enhanced detailed statistics with deduplication metrics"""
    try:
        stats = get_categorization_stats(postal_code)
        
        if stats:
            # Add file analysis
            restaurant_filename = f"restaurants_{postal_code}.json"
            store_filename = f"stores_{postal_code}.json"
            
            file_info = {
                'restaurants_file': {
                    'exists': os.path.exists(restaurant_filename),
                    'size_kb': round(os.path.getsize(restaurant_filename) / 1024, 1) if os.path.exists(restaurant_filename) else 0
                },
                'stores_file': {
                    'exists': os.path.exists(store_filename),
                    'size_kb': round(os.path.getsize(store_filename) / 1024, 1) if os.path.exists(store_filename) else 0
                }
            }
            
            # Analyze store brand diversity (deduplication effectiveness)
            store_diversity_analysis = {}
            if os.path.exists(store_filename):
                try:
                    with open(store_filename, 'r', encoding='utf-8') as f:
                        store_data = json.load(f)
                        
                    # Count brand occurrences
                    brand_counts = {}
                    for store in store_data:
                        name = store.get('name', 'N/A')
                        if name != 'N/A':
                            name_lower = name.lower()
                            for brand in ['franprix', 'carrefour', 'monoprix', 'casino', 'lidl']:
                                if brand in name_lower:
                                    brand_counts[brand.capitalize()] = brand_counts.get(brand.capitalize(), 0) + 1
                                    break
                    
                    store_diversity_analysis = {
                        'brand_counts': brand_counts,
                        'unique_brands': len(brand_counts),
                        'has_duplicates': any(count > 1 for count in brand_counts.values()),
                        'deduplication_needed': any(count > 1 for count in brand_counts.values())
                    }
                except:
                    pass
            
            return jsonify({
                'success': True,
                'postal_code': postal_code,
                'statistics': stats,
                'file_info': file_info,
                'quality_metrics': {
                    'avg_menu_items_per_restaurant': stats['restaurants'].get('avg_menu_items', 0),
                    'avg_products_per_store': stats['stores'].get('avg_products', 0),
                    'total_data_points': stats['totals']['total_items'],
                    'categorization_success': True if stats['totals']['establishments'] > 0 else False
                },
                'deduplication_analysis': store_diversity_analysis,  # NEW
                'carousel_effectiveness': {
                    'stores_with_products': len([s for s in stats['stores'].get('sample_names', []) if s]),
                    'estimated_carousels_used': stats['stores']['count'] * 2.5,  # Estimate based on typical store structure
                    'product_extraction_rate': 'High' if stats['stores'].get('avg_products', 0) > 20 else 'Medium' if stats['stores'].get('avg_products', 0) > 10 else 'Low'
                },
                'validation_approach': {
                    'method': 'simplified',
                    'description': 'Basic length and UI text filtering only',
                    'features': ['minimum_length_check', 'ui_text_filtering', 'data_testid_selector_trust']
                }
            })
        else:
            return jsonify({
                'success': False,
                'error': 'No data found for this postal code'
            }), 404
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/compare/<postal_code1>/<postal_code2>', methods=['GET'])
def compare_postal_codes(postal_code1, postal_code2):
    """Enhanced: Compare scraping results between two postal codes with deduplication analysis"""
    try:
        stats1 = get_categorization_stats(postal_code1)
        stats2 = get_categorization_stats(postal_code2)
        
        if not stats1 or not stats2:
            return jsonify({
                'success': False,
                'error': 'Data not found for one or both postal codes'
            }), 404
        
        comparison = {
            'postal_codes': {
                'first': postal_code1,
                'second': postal_code2
            },
            'restaurants': {
                'first_count': stats1['restaurants']['count'],
                'second_count': stats2['restaurants']['count'],
                'difference': stats2['restaurants']['count'] - stats1['restaurants']['count'],
                'percentage_change': round(((stats2['restaurants']['count'] - stats1['restaurants']['count']) / max(1, stats1['restaurants']['count'])) * 100, 1)
            },
            'stores': {
                'first_count': stats1['stores']['count'],
                'second_count': stats2['stores']['count'],
                'difference': stats2['stores']['count'] - stats1['stores']['count'],
                'percentage_change': round(((stats2['stores']['count'] - stats1['stores']['count']) / max(1, stats1['stores']['count'])) * 100, 1)
            },
            'total_items': {
                'first_total': stats1['totals']['total_items'],
                'second_total': stats2['totals']['total_items'],
                'difference': stats2['totals']['total_items'] - stats1['totals']['total_items']
            },
            'data_quality_comparison': {  # NEW
                'first_avg_products_per_store': stats1['stores'].get('avg_products', 0),
                'second_avg_products_per_store': stats2['stores'].get('avg_products', 0),
                'first_avg_menu_items_per_restaurant': stats1['restaurants'].get('avg_menu_items', 0),
                'second_avg_menu_items_per_restaurant': stats2['restaurants'].get('avg_menu_items', 0)
            }
        }
        
        return jsonify({
            'success': True,
            'comparison': comparison
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Enhanced health check endpoint with deduplication features"""
    return jsonify({
        'status': 'healthy',
        'service': 'UberEats Scraper API',
        'version': '6.0',  # Updated version for deduplication features
        'active_jobs': len(active_jobs),
        'uptime_minutes': int((datetime.now() - app_start_time).total_seconds() / 60),
        'features': [
            'restaurants', 
            'stores', 
            'store_carousel_navigation',
            'store_name_deduplication',     # NEW
            'url_based_duplicate_prevention', # NEW
            'simplified_validation',
            'background_processing', 
            'dual_file_output',
            'real_time_monitoring',
            'performance_metrics',
            'automatic_data_cleaning',
            'duplicate_prevention'
        ],
        'deduplication_features': {  # NEW section
            'store_name_normalization': True,
            'url_based_prevention': True,
            'supported_store_variations': ['Sprint', 'Express', 'City', 'Market', 'Super', 'Hyper'],
            'normalized_brands': ['Franprix', 'Carrefour', 'Monoprix', 'Casino', 'Lidl', 'Auchan'],
            'pre_scraping_check': True,
            'post_scraping_validation': True
        },
        'carousel_support': {
            'enabled': True,
            'max_clicks_per_carousel': 12,
            'timeout_seconds': 60,
            'duplicate_prevention': True
        },
        'detection_methods': [
            'data_testid_selector',
            'url_pattern_analysis',
            'page_title_keywords',
            'carousel_count_heuristic',
            'content_keyword_analysis'
        ],
        'validation_approach': {
            'type': 'simplified',
            'removed_features': ['price_detection', 'complex_product_validation'],
            'current_features': ['basic_length_check', 'ui_text_filtering', 'selector_trust']
        },
        'data_cleaning': {  
            'automatic_post_processing': True,
            'duplicate_removal': True,
            'price_filtering': True,
            'store_name_deduplication': True,  # NEW
            'manual_cleaning_endpoint': '/clean/{postal_code}'
        }
    })

@app.route('/', methods=['GET'])
def home():
    """API documentation with deduplication features"""
    return jsonify({
        'service': 'UberEats Scraper API',
        'version': '6.0',  # Updated version
        'tagline': 'Now with Store Name Deduplication & Enhanced Data Quality',
        'endpoints': {
            '/search': 'POST - Search for postal code only',
            '/scrape': 'POST - Start scraping in background (returns job_id)',
            '/job/<job_id>': 'GET - Get real-time job status and progress',
            '/jobs': 'GET - List all active jobs with progress',
            '/stop/<job_id>': 'POST - Stop a running job',
            '/status/<postal_code>': 'GET - Get comprehensive scraping status',
            '/stats/<postal_code>': 'GET - Get detailed statistics with performance metrics',
            '/analyze/<postal_code>': 'GET - Analyze data quality and get recommendations',  # NEW
            '/compare/<postal1>/<postal2>': 'GET - Compare results between postal codes',
            '/clean/<postal_code>': 'POST - Clean existing files (remove duplicates & invalid data)',
            '/health': 'GET - Health check with feature status'
        },
        'workflow': {
            '1. Start scraping': 'POST /scrape - Returns job_id',
            '2. Monitor progress': 'GET /job/{job_id} - Real-time updates with deduplication metrics',
            '3. Check results': 'GET /status/{postal_code} - Comprehensive results with brand analysis',
            '4. Analyze quality': 'GET /analyze/{postal_code} - Data quality analysis and recommendations',  # NEW
            '5. Clean if needed': 'POST /clean/{postal_code} - Manual cleaning with store deduplication'
        },
        'key_features': {
            'store_carousel_navigation': 'Automatically clicks through store product carousels',
            'store_name_deduplication': 'Prevents duplicate stores (e.g., Franprix vs Franprix Sprint)',  # NEW
            'url_based_duplicate_prevention': 'Skips already scraped URLs',  # NEW
            'simplified_validation': 'Removed complex price detection for better reliability',
            'dual_file_output': 'Separate JSON files for restaurants and stores',
            'real_time_monitoring': 'Live progress tracking and file monitoring',
            'performance_metrics': 'Speed, efficiency, and quality measurements',
            'automatic_cleaning': 'Post-processing removes duplicates and invalid data',
            'data_quality_analysis': 'Comprehensive analysis with actionable recommendations'  # NEW
        },
        'deduplication_system': {  # NEW section
            'store_name_normalization': {
                'description': 'Normalizes store names to prevent duplicates',
                'examples': {
                    'franprix_sprint': 'Normalizes to "franprix"',
                    'carrefour_city': 'Normalizes to "carrefour"',
                    'monoprix_express': 'Normalizes to "monoprix"'
                },
                'removed_suffixes': ['sprint', 'express', 'city', 'market', 'super', 'hyper', 'proximité']
            },
            'duplicate_prevention_levels': {
                '1_url_check': 'Prevents same URL from being scraped twice',
                '2_card_name_check': 'Pre-checks store names from cards before opening',
                '3_full_data_check': 'Final validation after scraping complete data',
                '4_tracking_update': 'Updates internal tracking to prevent future duplicates'
            },
            'effectiveness_monitoring': {
                'unique_brands_tracking': 'Tracks number of unique store brands found',
                'duplicate_prevention_count': 'Counts how many duplicates were prevented',
                'brand_distribution_analysis': 'Shows distribution of store brands'
            }
        },
        'data_structure': {
            'restaurants': {
                'file': 'restaurants_{postal}.json',
                'structure': 'menu_items with title and description',
                'fields': ['name', 'email', 'phone', 'registration_number', 'menu_items', 'menu_items_count']
            },
            'stores': {
                'file': 'stores_{postal}.json', 
                'structure': 'products with description only (deduplicated automatically)',  # Updated
                'fields': ['name', 'email', 'phone', 'registration_number', 'products', 'products_count'],
                'carousel_support': 'Automatically navigates through product categories',
                'deduplication': 'Store names normalized to prevent duplicates'  # NEW
            }
        },
        'technical_improvements': {
            'carousel_navigation': 'Up to 12 clicks per carousel with smart stopping',
            'store_name_deduplication': 'Advanced name normalization prevents brand duplicates',  # NEW
            'url_duplicate_prevention': 'URL-based tracking prevents re-scraping same establishments',  # NEW
            'enhanced_selectors': 'Multiple fallback CSS selectors for robustness',
            'simplified_filtering': 'Removed complex price detection for reliability',
            'performance_monitoring': 'Real-time metrics and file size tracking',
            'validation_approach': 'Trust data-testid selectors, basic length checks only',
            'automatic_post_processing': 'Files automatically cleaned after scraping',
            'price_pattern_removal': 'Removes price-only and quantity-only descriptions'
        },
        'data_quality': {
            'automatic_cleaning': 'Files are automatically cleaned after each scraping session',
            'store_duplicate_removal': 'Removes duplicate stores by normalized name',  # NEW
            'url_duplicate_removal': 'Removes duplicate establishments by URL',  # NEW
            'invalid_data_filtering': 'Removes price-only descriptions like "(4,80 €/kg)"',
            'quantity_filtering': 'Removes quantity-only descriptions like "6 pcs • 330 ml"',
            'manual_cleaning': 'Manual cleaning available via /clean/{postal_code} endpoint',
            'data_analysis': 'Quality analysis and recommendations via /analyze/{postal_code}'  # NEW
        },
        'monitoring_enhancements': {  # NEW section
            'real_time_deduplication_tracking': 'Track duplicates prevented during scraping',
            'brand_diversity_analysis': 'Monitor unique store brands found',
            'deduplication_effectiveness_metrics': 'Measure how well deduplication is working',
            'store_name_variety_tracking': 'Track variety of store names and brands'
        },
        'removed_complexity': {
            'price_detection': 'Removed complex price pattern matching',
            'product_validation': 'Removed hard-coded product description validation', 
            'complexity_reason': 'Simplified approach for better reliability and fewer false negatives'
        },
        'reliability_improvements': {
            'selector_trust': 'Trust data-testid="store-item-thumbnail-label" as strong indicator',
            'reduced_false_negatives': 'Less aggressive filtering means more valid products captured',
            'faster_processing': 'Removed expensive regex operations for better performance',
            'post_processing_cleaning': 'Automatic cleanup ensures high data quality',
            'intelligent_deduplication': 'Smart store name matching prevents duplicate scraping'  # NEW
        }
    })

# Global app start time
app_start_time = datetime.now()

if __name__ == '__main__':
    print("Starting UberEats Scraper API v6.0...")
    print("[] Key Features:")
    print("   • Store carousel navigation")
    print("   • Store name deduplication (NEW)")      # NEW
    print("   • URL-based duplicate prevention (NEW)") # NEW
    print("   • Simplified validation (no complex price detection)") 
    print("   • Dual JSON file output")
    print("   • Real-time progress monitoring")
    print("   • Performance metrics")
    print("   • Automatic data cleaning")
    print("   • Brand diversity analysis (NEW)")       # NEW
    print("   • Price/quantity filtering")
    print("\n\n[] Deduplication System:")               # NEW section
    print("   • Store Name Normalization: Franprix Sprint → franprix")
    print("   • URL Tracking: Prevents re-scraping same establishments")
    print("   • Multi-level Checks: Card preview + full data validation")
    print("   • Brand Analysis: Tracks unique store brands found")
    print("   • Effectiveness Monitoring: Real-time duplicate prevention metrics")
    print("\n\n[] API Endpoints:")
    print("   • POST /scrape - Start scraping with deduplication")
    print("   • GET /job/{id} - Real-time progress with duplicate metrics")
    print("   • GET /status/{postal} - Results with brand analysis")
    print("   • GET /stats/{postal} - Detailed analytics with deduplication metrics")
    print("   • GET /analyze/{postal} - Data quality analysis (NEW)")  # NEW
    print("   • GET /compare/{postal1}/{postal2} - Compare results")
    print("   • POST /clean/{postal} - Manual file cleaning with store deduplication")  
    print("\n\n[] Data Quality:")
    print("   • Automatic: Post-processing after each scraping session")
    print("   • Store Deduplication: Normalizes names to prevent duplicates (NEW)")
    print("   • URL Deduplication: Prevents re-scraping same establishments (NEW)")
    print("   • Manual: /clean/{postal_code} endpoint for on-demand cleaning")
    print("   • Removes: Duplicates, price-only descriptions, invalid data")
    print("   • Preserves: All valid product and menu item data")
    print("   • Analysis: /analyze/{postal_code} for quality recommendations (NEW)")
    print("\n\n[] Validation Approach: Simplified")
    print("   • Removed: Complex price detection")
    print("   • Removed: Hard-coded product validation")
    print("   • Kept: Basic length checks + UI filtering")
    print("   • Added: Automatic post-processing cleanup")
    print("   • Added: Intelligent store name deduplication (NEW)")
    print("   • Trust: data-testid selectors as primary indicators")
    app.run(debug=False, host='0.0.0.0', port=5005, threaded=True)

