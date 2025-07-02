import time
import re
from selenium.webdriver.common.by import By


def scrape_menu_for_restaurant(driver, max_items=None):
    """
    Optimized menu scraper - uses only the proven working method
    """
    try:
        print(f"[*] Starting optimized menu scraping (max: {max_items or 'unlimited'})...")
        
        # Quick scroll to load content
        scroll_page_quickly(driver)
        
        # Use the working HTML pattern method directly
        menu_items = extract_with_working_pattern(driver, max_items)
        
        # Only if that fails, try the DOM method as backup
        if len(menu_items) == 0:
            print(f"[*] HTML pattern failed, trying DOM backup...")
            menu_items = extract_with_dom_backup(driver, max_items)
        
        print(f"[✓] Menu scraping complete: {len(menu_items)} items extracted")
        return menu_items
        
    except Exception as e:
        print(f"[!] Menu scraping failed: {e}")
        return []


def scroll_page_quickly(driver):
    """Quick scroll to load menu content"""
    try:
        # Fast scroll to bottom and back
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.5)
    except Exception as e:
        print(f"[!] Scroll error: {e}")


def extract_with_working_pattern(driver, max_items=None):
    """Use the proven working Pattern 1 from HTML source"""
    try:
        page_source = driver.page_source
        menu_items = []
        
        # Pattern 1 that worked: JSON-like name/description pattern
        pattern = r'"name"\s*:\s*"([^"]{5,60})".*?"description"\s*:\s*"([^"]{15,200})"'
        
        matches = re.findall(pattern, page_source, re.IGNORECASE | re.DOTALL)
        print(f"[+] Working pattern found {len(matches)} matches")
        
        for match in matches:
            if max_items and len(menu_items) >= max_items:
                break
            
            title = clean_text(match[0])
            description = clean_text(match[1])
            
            # Quick validation
            if len(title) > 3 and len(description) > 10:
                menu_item = {
                    "title": title,
                    "description": description,
                    "link": "N/A"
                }
                
                menu_items.append(menu_item)
                print(f"[✓] Menu {len(menu_items)}: {title[:40]}...")
        
        return menu_items
        
    except Exception as e:
        print(f"[!] Working pattern extraction failed: {e}")
        return []


def extract_with_dom_backup(driver, max_items=None):
    """Backup DOM method using div[data-testid*="store-item"] selector"""
    try:
        print(f"[*] Using DOM backup with store-item selector...")
        
        # Use the selector that found 80 elements
        elements = driver.find_elements(By.CSS_SELECTOR, 'div[data-testid*="store-item"]')
        print(f"[+] Found {len(elements)} store-item elements")
        
        menu_items = []
        processed_texts = set()
        
        for element in elements[:50]:  # Limit to first 50 to avoid processing too many
            try:
                element_text = element.text.strip()
                
                if not element_text or len(element_text) < 10:
                    continue
                
                # Skip duplicates
                if element_text in processed_texts:
                    continue
                
                # Parse element text
                title, description = parse_element_text_simple(element_text)
                
                if title and description and len(title) > 3:
                    menu_item = {
                        "title": title,
                        "description": description,
                        "link": "N/A"
                    }
                    
                    menu_items.append(menu_item)
                    processed_texts.add(element_text)
                    print(f"[✓] DOM Menu {len(menu_items)}: {title[:40]}...")
                    
                    if max_items and len(menu_items) >= max_items:
                        break
                        
            except Exception:
                continue
        
        return menu_items
        
    except Exception as e:
        print(f"[!] DOM backup failed: {e}")
        return []


def parse_element_text_simple(element_text):
    """Simple parsing of element text into title and description"""
    try:
        lines = [line.strip() for line in element_text.split('\n') if line.strip()]
        
        if len(lines) >= 2:
            title = lines[0]
            
            # Find best description line (longest one with food indicators)
            description = ""
            for line in lines[1:]:
                if len(line) > 15:
                    # Prefer lines with food-related content
                    if any(word in line.lower() for word in [',', 'sauce', 'cheese', 'with', 'served']):
                        description = line
                        break
                    elif not description:  # Fallback to first decent line
                        description = line
            
            if not description and len(lines) > 1:
                description = lines[1]
            
            return title, description if description else "N/A"
        
        return "", ""
        
    except Exception:
        return "", ""


def clean_text(text):
    """Clean and normalize text"""
    if not text:
        return ""
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Decode common HTML entities
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&apos;', "'").replace('&nbsp;', ' ')
    text = text.replace('&euro;', '€')
    
    # Remove extra whitespace and quotes
    text = ' '.join(text.split()).strip('"\'')
    
    # Remove common unwanted phrases
    unwanted = ['add to cart', 'order now', 'select options']
    for phrase in unwanted:
        text = text.replace(phrase, '').strip()
    
    return text


# Keep simple fallback for testing
def simple_menu_extraction(driver, max_items=None):
    """Ultra-simple extraction for testing"""
    try:
        print(f"[*] Simple fallback extraction...")
        
        all_text = driver.find_element(By.TAG_NAME, 'body').text
        lines = [line.strip() for line in all_text.split('\n') if line.strip()]
        
        menu_items = []
        
        # Look for obvious menu-related lines
        for line in lines:
            if (10 < len(line) < 80 and 
                any(word in line.lower() for word in ['menu', 'burger', 'chicken', 'pizza', 'wrap', 'sandwich'])):
                
                menu_items.append({
                    "title": line,
                    "description": "Simple extraction",
                    "link": "N/A"
                })
                
                if max_items and len(menu_items) >= max_items:
                    break
        
        print(f"[✓] Simple extraction: {len(menu_items)} items")
        return menu_items
        
    except Exception as e:
        print(f"[!] Simple extraction failed: {e}")
        return []