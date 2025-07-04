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
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True)