import time
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.common.action_chains import ActionChains


def scrape_menu_for_restaurant(driver, max_items=None):
    """
    OPTIMIZED: Restaurant menu scraper - returns items with TITLE + DESCRIPTION
    """
    try:
        print(f"[*] Starting restaurant menu scraping (max: {max_items or 'unlimited'})...")
        
        scroll_page_efficiently(driver)
        
        # Try regex extraction first
        menu_items = extract_with_regex_patterns(driver, max_items, "restaurant")
        
        # Fallback to DOM extraction
        if len(menu_items) == 0:
            menu_items = extract_with_dom_fallback(driver, max_items, "restaurant")
        
        # Final fallback to simple text extraction
        if len(menu_items) == 0:
            menu_items = extract_restaurant_simple_fallback(driver, max_items)
        
        # Clean and validate
        validated_items = clean_restaurant_items(menu_items)
        
        print(f"[✓] Restaurant menu scraping complete: {len(validated_items)} items")
        return validated_items
        
    except Exception as e:
        print(f"[!] Restaurant menu scraping failed: {e}")
        return []


def scrape_store_with_carousels(driver, max_items=None):
    """
    UPDATED: This function is now handled by the main scraper's optimized method
    Keep for compatibility but redirect to simple extraction
    """
    try:
        print(f"[*] Store scraping delegated to main scraper's optimized method...")
        
        # Simple fallback extraction for stores (main scraper handles the optimized version)
        products = extract_store_simple_fallback(driver, max_items)
        
        print(f"[✓] Store scraping complete: {len(products)} products")
        return products
        
    except Exception as e:
        print(f"[!] Store scraping failed: {e}")
        return []


def extract_with_regex_patterns(driver, max_items=None, item_type="restaurant"):
    """
    SIMPLIFIED: Improved regex extraction without price validation
    """
    try:
        time.sleep(2)
        page_source = driver.page_source
        items = []
        
        # Enhanced patterns for better extraction
        if item_type == "restaurant":
            patterns = [
                r'"name"\s*:\s*"([^"]{3,80})".*?"description"\s*:\s*"([^"]{10,300})"',
                r'"title"\s*:\s*"([^"]{3,80})".*?"description"\s*:\s*"([^"]{10,300})"',
                r'"itemName"\s*:\s*"([^"]{3,80})".*?"itemDescription"\s*:\s*"([^"]{10,300})"',
                r'"displayName"\s*:\s*"([^"]{3,80})".*?"description"\s*:\s*"([^"]{10,300})"'
            ]
        else:  # store
            patterns = [
                r'"description"\s*:\s*"([^"]{20,400})"',
                r'"longDescription"\s*:\s*"([^"]{20,400})"',
                r'"productDescription"\s*:\s*"([^"]{20,400})"',
                r'"subtitle"\s*:\s*"([^"]{20,400})"'
            ]
        
        for pattern_idx, pattern in enumerate(patterns):
            try:
                matches = re.findall(pattern, page_source, re.IGNORECASE | re.DOTALL)
                if matches:
                    print(f"[+] Pattern {pattern_idx + 1} found {len(matches)} matches")
                    
                    for match in matches:
                        if max_items and len(items) >= max_items:
                            break
                        
                        if item_type == "restaurant":
                            title = clean_text_simple(match[0])
                            description = clean_text_simple(match[1])
                            
                            # Simplified validation - basic length checks only
                            if len(title) > 2 and len(description) > 8:
                                items.append({
                                    "title": title,
                                    "description": description,
                                    "link": "N/A"
                                })
                        else:  # store
                            description = clean_text_simple(match if isinstance(match, str) else match[0])
                            
                            if len(description) > 20:
                                items.append({
                                    "description": description
                                })
                    
                    if items:
                        break
                        
            except Exception as e:
                print(f"[!] Pattern {pattern_idx + 1} failed: {e}")
                continue
        
        return items
        
    except Exception as e:
        print(f"[!] Regex extraction failed: {e}")
        return []


def extract_with_dom_fallback(driver, max_items=None, item_type="restaurant"):
    """
    ENHANCED: Better DOM extraction with more selectors
    """
    try:
        time.sleep(2)
        
        if item_type == "restaurant":
            selectors = [
                'div[data-testid*="store-item"]',
                'div[data-testid*="menu-item"]',
                'div[data-testid*="item"]',
                'div[class*="menu-item"]',
                'div[class*="item-card"]',
                'li[class*="menu-item"]',
                'div[class*="dish"]'
            ]
        else:  # store
            selectors = [
                'div[data-testid="store-item-thumbnail-label"]',  # Your specific selector
                'div[data-testid*="store-item"]',
                'div[data-testid*="product"]',
                'div[class*="product"]',
                'div[class*="item"]'
            ]
        
        elements = []
        used_selector = ""
        
        for selector in selectors:
            try:
                found_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if len(found_elements) > 2:  # Need at least a few items
                    elements = found_elements
                    used_selector = selector
                    print(f"[+] Using DOM selector: {selector} ({len(elements)} elements)")
                    break
            except:
                continue
        
        if not elements:
            print(f"[!] No elements found with DOM selectors")
            return []
        
        items = []
        processed_texts = set()
        
        for element in elements[:50]:  # Limit processing
            try:
                element_text = element.text.strip()
                if not element_text or len(element_text) < 10:
                    continue
                
                if element_text in processed_texts:
                    continue
                
                processed_texts.add(element_text)
                
                if item_type == "restaurant":
                    title, description = parse_element_text_simple(element_text)
                    if title and description and len(title) > 2:
                        items.append({
                            "title": title,
                            "description": description,
                            "link": "N/A"
                        })
                else:  # store
                    description = extract_description_from_text(element_text)
                    if description and len(description) > 15:
                        items.append({
                            "description": description
                        })
                
                if max_items and len(items) >= max_items:
                    break
                    
            except:
                continue
        
        return items
        
    except Exception as e:
        print(f"[!] DOM extraction failed: {e}")
        return []

def normalize_store_name(name):
    """
    Normalize store names for comparison (consistent with scrape.py)
    """
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

def extract_restaurant_simple_fallback(driver, max_items=None):
    """
    SIMPLIFIED: Simple fallback for restaurants without price validation
    """
    try:
        print(f"[*] Using restaurant simple fallback extraction...")
        
        # Get all text from page
        try:
            body_text = driver.find_element(By.TAG_NAME, 'body').text
        except:
            return []
        
        lines = [line.strip() for line in body_text.split('\n') if line.strip()]
        menu_items = []
        processed_lines = set()
        
        for i, line in enumerate(lines):
            try:
                if len(line) < 5 or len(line) > 100:  # Reasonable title length
                    continue
                
                if line in processed_lines:
                    continue
                
                # Skip if it's clearly UI element
                if is_ui_text_simple(line):
                    continue
                
                # This could be a menu item title
                title = clean_text_simple(line)
                description = ""
                
                # Look for description in next few lines
                for j in range(i + 1, min(i + 4, len(lines))):
                    candidate_desc = lines[j].strip()
                    
                    if (len(candidate_desc) > 15 and 
                        not is_ui_text_simple(candidate_desc) and
                        candidate_desc not in processed_lines):
                        
                        description = clean_text_simple(candidate_desc)
                        break
                
                if title and len(title) > 3:
                    menu_items.append({
                        "title": title,
                        "description": description if description else "N/A",
                        "link": "N/A"
                    })
                    
                    processed_lines.add(line)
                    if description:
                        processed_lines.add(description)
                    
                    if max_items and len(menu_items) >= max_items:
                        break
                        
            except:
                continue
        
        return menu_items
        
    except Exception as e:
        print(f"[!] Restaurant simple fallback failed: {e}")
        return []


def extract_store_simple_fallback(driver, max_items=None):
    """
    UPDATED: Store extraction with improved filtering and deduplication
    """
    try:
        print(f"[*] Using store simple fallback extraction (span-targeted)...")
        
        products = []
        seen_descriptions = set()  # Track during extraction
        
        # Method 1: Look for spans inside store-item containers
        try:
            store_containers = driver.find_elements(By.CSS_SELECTOR, 'div[data-testid="store-item-thumbnail-label"]')
            print(f"[*] Found {len(store_containers)} store item containers")
            
            for container in store_containers:
                try:
                    spans = container.find_elements(By.CSS_SELECTOR, 'span[data-testid="rich-text"]')
                    for span in spans:
                        text_content = span.text.strip()
                        if text_content and len(text_content) > 5:
                            description = clean_text_simple(text_content)
                            
                            # Enhanced filtering
                            if (len(description) > 8 and
                                not is_ui_text_simple(description) and
                                not is_likely_price_or_quantity(description) and
                                description not in seen_descriptions):  # Avoid duplicates during extraction
                                
                                products.append({
                                    "description": description
                                })
                                seen_descriptions.add(description)
                                
                                if max_items and len(products) >= max_items:
                                    break
                except Exception:
                    continue
                    
                if max_items and len(products) >= max_items:
                    break
                    
        except Exception as e:
            print(f"[!] Method 1 failed: {e}")
        
        # Method 2: Fallback methods (similar enhancements)
        if not products:
            print(f"[*] Fallback: Looking for all rich-text spans...")
            try:
                all_spans = driver.find_elements(By.CSS_SELECTOR, 'span[data-testid="rich-text"]')
                print(f"[*] Found {len(all_spans)} rich-text spans")
                
                for span in all_spans:
                    try:
                        text_content = span.text.strip()
                        if (text_content and 
                            len(text_content) > 8 and
                            len(text_content) < 300 and
                            not is_ui_text_simple(text_content) and
                            not is_likely_price_or_quantity(text_content)):
                            
                            description = clean_text_simple(text_content)
                            if description and len(description) > 8 and description not in seen_descriptions:
                                products.append({
                                    "description": description
                                })
                                seen_descriptions.add(description)
                                
                                if max_items and len(products) >= max_items:
                                    break
                    except Exception:
                        continue
                        
            except Exception as e:
                print(f"[!] Method 2 failed: {e}")
        
        # Method 3: Final fallback (similar enhancements)
        if not products:
            print(f"[*] Final fallback: All meaningful spans...")
            try:
                all_spans = driver.find_elements(By.TAG_NAME, 'span')
                print(f"[*] Found {len(all_spans)} total spans")
                
                for span in all_spans[:200]:
                    try:
                        text_content = span.text.strip()
                        if (text_content and 
                            len(text_content) > 10 and
                            len(text_content) < 200 and
                            not is_ui_text_simple(text_content) and
                            not is_likely_price_or_quantity(text_content)):
                            
                            description = clean_text_simple(text_content)
                            if description and len(description) > 10 and description not in seen_descriptions:
                                products.append({
                                    "description": description
                                })
                                seen_descriptions.add(description)
                                
                                if max_items and len(products) >= max_items:
                                    break
                    except Exception:
                        continue
                        
            except Exception as e:
                print(f"[!] Method 3 failed: {e}")
        
        # Final duplicate removal (extra safety)
        unique_products = remove_duplicates(products, "store")
        
        print(f"[✓] Store fallback extraction complete: {len(unique_products)} unique products")
        return unique_products
        
    except Exception as e:
        print(f"[!] Store simple fallback failed: {e}")
        return []


def is_likely_price_or_quantity(text):
    """
    ENHANCED: Better price/quantity detection
    """
    if not text or len(text.strip()) < 3:
        return False
    
    text = text.strip()
    
    # Very obvious price patterns
    obvious_price_patterns = [
        r'^\(\d+[,\.]\d+\s*€[^a-zA-Z]*\)$',  # (4,80 €/kg) etc.
        r'^\d+\s*pcs?\s*•\s*\d+',  # 6 pcs • 330 ml
        r'^\d+\s*x\s*\d+',  # 4 x 500ml
        r'^\(\d+\s*pcs?\)$',  # (6 pcs)
        r'^\(\d+[,\.]\d+\s*€/\w+\)$',  # (14,70 €/kg)
        r'^\d+\s*pcs\s*•\s*\d+\s*\w+$',  # 9 pcs • 23.5 g
    ]
    
    for pattern in obvious_price_patterns:
        if re.match(pattern, text, re.IGNORECASE):
            return True
    
    # If it's mostly numbers and symbols, likely price
    if re.match(r'^[\d\s€,.()/-]+$', text) and len(text) < 20:
        return True
    
    # Check for standalone quantity patterns
    if re.match(r'^\d+\s*pcs?$', text, re.IGNORECASE):  # "6 pcs"
        return True
    
    return False

    
def parse_element_text_simple(element_text):
    """
    SIMPLIFIED: Better text parsing for restaurants without price checks
    """
    try:
        lines = [line.strip() for line in element_text.split('\n') if line.strip()]
        
        if len(lines) < 1:
            return "", ""
        
        # First non-UI line is usually the title
        title = ""
        description = ""
        
        for line in lines:
            if len(line) > 2 and not is_ui_text_simple(line):
                if not title:
                    title = line
                elif not description and len(line) > 10:
                    description = line
                    break
        
        return title, description
        
    except:
        return "", ""


def extract_description_from_text(element_text):
    """
    SIMPLIFIED: Better description extraction for stores without price checks
    """
    try:
        lines = [line.strip() for line in element_text.split('\n') if line.strip()]
        
        # Find the best description line
        best_description = ""
        
        for line in lines:
            if (len(line) > len(best_description) and 
                len(line) > 15 and 
                not is_ui_text_simple(line)):
                best_description = line
        
        return best_description
        
    except:
        return ""


def is_ui_text_simple(text):
    """
    ENHANCED: Better UI text detection
    """
    if not text:
        return False
    
    text_lower = text.lower().strip()
    
    # Common UI elements
    ui_patterns = [
        'ajouter', 'add', 'commander', 'order', 'voir plus', 'show more',
        'disponible', 'available', 'en stock', 'in stock', 'select',
        'choisir', 'options', 'quantity', 'quantité', 'personnaliser',
        'customize', 'modify', 'modifier', 'delete', 'supprimer',
        'click here', 'cliquez ici', 'buy now', 'acheter maintenant',
        'add to cart', 'ajouter au panier', 'livraison', 'delivery',
        'retrait', 'pickup', 'commander maintenant', 'order now',
        'menu', 'accueil', 'home', 'contact', 'about', 'login',
        'connexion', 'inscription', 'register', 'sign up'
    ]
    
    # Check if text is exactly a UI element
    if text_lower in ui_patterns:
        return True
    
    # Check if text contains UI elements (for short texts)
    if len(text) < 30:
        return any(ui_word in text_lower for ui_word in ui_patterns)
    
    return False


def clean_text_simple(text):
    """
    ENHANCED: Better text cleaning
    """
    if not text:
        return ""
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Enhanced HTML entities
    html_entities = {
        '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', 
        '&apos;': "'", '&nbsp;': ' ', '&euro;': '€', '&copy;': '©', 
        '&reg;': '®', '&trade;': '™', '&hellip;': '...', '&mdash;': '—',
        '&ndash;': '–', '&lsquo;': ''', '&rsquo;': ''', '&ldquo;': '"',
        '&rdquo;': '"', '&bull;': '•', '&middot;': '·'
    }
    
    for entity, replacement in html_entities.items():
        text = text.replace(entity, replacement)
    
    # Clean whitespace and special characters
    text = ' '.join(text.split()).strip()
    text = text.strip('"\'.,;:!?()[]{}')
    
    return text


def clean_restaurant_items(items):
    """
    UPDATED: Better restaurant item validation with store name detection
    """
    cleaned_items = []
    
    for item in items:
        title = item.get("title", "").strip()
        description = item.get("description", "").strip()
        
        # Enhanced validation
        if (len(title) > 2 and 
            not is_ui_text_simple(title) and
            not is_likely_price_or_quantity(title)):
            
            # Check if title looks like a store name (could indicate misclassification)
            normalized_title = normalize_store_name(title)
            store_indicators = ['franprix', 'carrefour', 'monoprix', 'casino', 'lidl', 'auchan', 'leclerc']
            
            is_likely_store_name = any(indicator in normalized_title for indicator in store_indicators)
            
            if not is_likely_store_name:  # Only add if it's not a store name
                # Also check description if it exists
                if description and description != "N/A":
                    if is_likely_price_or_quantity(description):
                        description = "N/A"  # Clear obviously bad descriptions
                
                cleaned_items.append({
                    "title": title,
                    "description": description if description and description != "N/A" else "N/A",
                    "link": item.get("link", "N/A")
                })
            else:
                print(f"[*] Filtered out potential store name from restaurant items: {title}")
    
    return cleaned_items

def remove_duplicates(items, item_type):
    """
    ENHANCED: Better duplicate removal with store name normalization
    """
    seen = set()
    unique_items = []
    
    for item in items:
        if item_type == "restaurant":
            key = item.get("title", "")
        else:
            key = item.get("description", "")
        
        # Normalize key for comparison
        normalized_key = clean_text_simple(key).lower()
        
        # Additional normalization for store items that might be store names
        if item_type == "store" and normalized_key:
            # Check if this looks like a store name pattern
            if any(brand in normalized_key for brand in ['franprix', 'carrefour', 'monoprix', 'casino', 'lidl']):
                normalized_key = normalize_store_name(normalized_key)
        
        if normalized_key and normalized_key not in seen and len(normalized_key) > 5:
            seen.add(normalized_key)
            unique_items.append(item)
    
    return unique_items

def scroll_page_efficiently(driver):
    """
    OPTIMIZED: More efficient scrolling
    """
    try:
        time.sleep(1)  # Reduced initial wait
        
        # Faster scrolling pattern
        for i in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.8)  # Reduced wait time
        
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.5)  # Reduced wait time
        
    except Exception as e:
        print(f"[!] Scroll failed: {e}")

def detect_establishment_type_from_items(menu_items):
    """
    ENHANCED: Better type detection with store name analysis
    """
    if not menu_items:
        return "unknown"
    
    # Existing structure-based heuristics
    description_only_count = 0
    title_description_count = 0
    
    for item in menu_items:
        title = item.get("title", "")
        
        if title and title != "N/A":
            title_description_count += 1
        else:
            description_only_count += 1
    
    # If most items have only descriptions, likely a store
    if description_only_count > title_description_count:
        return "store"
    
    # Otherwise, likely a restaurant
    return "restaurant"

def wait_for_element_safely(driver, locator, timeout=10):
    """Simple element waiting"""
    try:
        wait = WebDriverWait(driver, timeout)
        return wait.until(EC.presence_of_element_located(locator))
    except:
        return None


def get_element_text_safely(element):
    """Simple text extraction"""
    try:
        return element.text.strip() if element else ""
    except:
        return ""


def scroll_element_into_view_safely(driver, element):
    """Simple element scrolling"""
    try:
        if element and element.is_displayed():
            return True
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(1)
        return True
    except:
        return False


def handle_page_loading_safely(driver, timeout=30):
    """Simple page loading check"""
    try:
        WebDriverWait(driver, timeout).until(
            lambda driver: driver.execute_script("return document.readyState") == "complete"
        )
        time.sleep(2)
        return True
    except:
        return False


def retry_with_backoff(func, max_retries=3, base_delay=1):
    """Simple retry mechanism"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(base_delay * (2 ** attempt))
    return None


def check_memory_usage():
    """Simple memory check"""
    try:
        import psutil
        return psutil.virtual_memory().percent
    except:
        return None


def optimize_driver_for_scraping(driver):
    """Simple driver optimization"""
    try:
        driver.implicitly_wait(10)
        driver.set_page_load_timeout(30)
        driver.set_window_size(1920, 1080)
        return True
    except:
        return False


def validate_scraped_data(data, data_type="restaurant"):
    """
    UPDATED: Better data validation with price filtering
    """
    validated = []
    
    for item in data:
        if data_type == "restaurant":
            title = item.get('title', '').strip()
            description = item.get('description', '').strip()
            
            if (title and len(title) > 2 and 
                not is_ui_text_simple(title) and
                not is_likely_price_or_quantity(title)):  # New check
                
                # Clean description if it's obviously bad
                if description and is_likely_price_or_quantity(description):
                    description = "N/A"
                
                validated.append({
                    'title': title,
                    'description': description if description else 'N/A',
                    'link': item.get('link', 'N/A')
                })
        else:  # store
            description = item.get('description', '').strip()
            
            if (description and len(description) > 15 and 
                not is_ui_text_simple(description) and
                not is_likely_price_or_quantity(description)):  # New check
                validated.append({
                    'description': description
                })
    
    return validated

def log_scraping_stats(items, establishment_type):
    """Enhanced logging"""
    print(f"[+] {establishment_type.upper()} items: {len(items)}")
    
    if items:
        sample = items[0]
        if establishment_type == "restaurant":
            title = sample.get('title', 'N/A')
            desc = sample.get('description', 'N/A')
            print(f"[+] Sample: {title[:30]}... | {desc[:30]}...")
        else:
            desc = sample.get('description', 'N/A')
            print(f"[+] Sample: {desc[:50]}...")


def scrape_establishment_items(driver, establishment_type, max_items=None):
    """Main scraping function"""
    try:
        if establishment_type == "restaurant":
            items = scrape_menu_for_restaurant(driver, max_items)
        elif establishment_type == "store":
            items = scrape_store_with_carousels(driver, max_items)
        else:
            return []
        
        validated_items = validate_scraped_data(items, establishment_type)
        log_scraping_stats(validated_items, establishment_type)
        
        return validated_items
        
    except Exception as e:
        print(f"[!] Scraping failed: {e}")
        return []