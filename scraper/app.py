from flask import Flask, request, jsonify
from main import perform_search, perform_full_scrape
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
        self.progress = {"restaurants_scraped": 0, "current_restaurant": "", "error": None}
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
    """Run scraping in background thread"""
    try:
        job.status = "running"
        active_jobs[job.job_id] = job
        
        print(f"[*] Starting background scraping job {job.job_id}")
        
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
        
        print(f"[*] Background job {job.job_id} completed with status: {job.status}")
        
    except Exception as e:
        job.status = "failed"
        job.progress["error"] = str(e)
        print(f"[!] Background job {job.job_id} failed: {e}")
    
    finally:
        # Keep job info for 1 hour after completion
        time.sleep(3600)
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
def scrape_restaurants():
    """Start scraping in background and return job ID"""
    try:
        data = request.get_json()
        postal_code = data.get('postal_code')
        visible = data.get('visible', False)
        max_restaurants = data.get('max_restaurants', None)
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

        # Create background job
        job_id = str(uuid.uuid4())[:8]  # Short job ID
        job = ScrapingJob(job_id, postal_code, visible, max_restaurants, max_menu_items)
        
        # Start background thread
        thread = threading.Thread(target=run_scraping_job, args=(job,))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'job_id': job_id,
            'message': f'Scraping started in background for postal code {postal_code}',
            'status_url': f'/job/{job_id}',
            'estimated_time': 'This may take 1-3 hours depending on the number of restaurants'
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/job/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Get status of a scraping job"""
    try:
        if job_id not in active_jobs:
            return jsonify({
                'success': False,
                'error': f'Job {job_id} not found or expired'
            }), 404
        
        job = active_jobs[job_id]
        
        # Add current file status
        filename = f"restaurants_{job.postal_code}.json"
        current_count = 0
        current_menu_count = 0
        
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    current_count = len(existing_data)
                    current_menu_count = sum(len(item.get('menu_items', [])) for item in existing_data)
            except:
                pass
        
        response = job.to_dict()
        response.update({
            'current_restaurants_saved': current_count,
            'current_menu_items_saved': current_menu_count,
            'output_file': filename,
            'runtime_minutes': int((datetime.now() - job.start_time).total_seconds() / 60)
        })
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/jobs', methods=['GET'])
def list_active_jobs():
    """List all active jobs"""
    try:
        jobs_list = []
        for job_id, job in active_jobs.items():
            jobs_list.append({
                'job_id': job_id,
                'postal_code': job.postal_code,
                'status': job.status,
                'start_time': job.start_time.isoformat(),
                'runtime_minutes': int((datetime.now() - job.start_time).total_seconds() / 60)
            })
        
        return jsonify({
            'success': True,
            'active_jobs': jobs_list,
            'total_jobs': len(jobs_list)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/stop/<job_id>', methods=['POST'])
def stop_job(job_id):
    """Stop a running job (basic implementation)"""
    try:
        if job_id not in active_jobs:
            return jsonify({
                'success': False,
                'error': f'Job {job_id} not found'
            }), 404
        
        job = active_jobs[job_id]
        job.status = "stopped"
        
        return jsonify({
            'success': True,
            'message': f'Job {job_id} marked for stopping'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/status/<postal_code>', methods=['GET'])
def get_scraping_status(postal_code):
    """Get current scraping status for a postal code"""
    try:
        filename = f"restaurants_{postal_code}.json"
        
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                
            total_restaurants = len(existing_data)
            
            # Get some sample restaurant names
            sample_names = [
                item.get('name', 'N/A') for item in existing_data[:5] 
                if item.get('name') and item.get('name') != 'N/A'
            ]
            
            # Count total menu items across all restaurants
            total_menu_items = sum(
                len(item.get('menu_items', [])) for item in existing_data
            )
            
            # Check if there's an active job for this postal code
            active_job_info = None
            for job_id, job in active_jobs.items():
                if job.postal_code == postal_code:
                    active_job_info = {
                        'job_id': job_id,
                        'status': job.status,
                        'runtime_minutes': int((datetime.now() - job.start_time).total_seconds() / 60)
                    }
                    break
            
            return jsonify({
                'success': True,
                'postal_code': postal_code,
                'total_restaurants': total_restaurants,
                'total_menu_items': total_menu_items,
                'sample_restaurants': sample_names,
                'file_exists': True,
                'filename': filename,
                'active_job': active_job_info
            })
        else:
            return jsonify({
                'success': True,
                'postal_code': postal_code,
                'total_restaurants': 0,
                'total_menu_items': 0,
                'sample_restaurants': [],
                'file_exists': False,
                'filename': filename,
                'active_job': None
            })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'UberEats Scraper API',
        'active_jobs': len(active_jobs),
        'uptime_minutes': int((datetime.now() - app_start_time).total_seconds() / 60)
    })

@app.route('/', methods=['GET'])
def home():
    """API documentation"""
    return jsonify({
        'service': 'UberEats Scraper API',
        'version': '3.0',  # Updated version
        'endpoints': {
            '/search': 'POST - Search for postal code only',
            '/scrape': 'POST - Start scraping in background (returns job_id)',
            '/job/<job_id>': 'GET - Get job status and progress',
            '/jobs': 'GET - List all active jobs',
            '/stop/<job_id>': 'POST - Stop a running job',
            '/status/<postal_code>': 'GET - Get current scraping status',
            '/health': 'GET - Health check'
        },
        'workflow': {
            '1. Start scraping': 'POST /scrape - Returns job_id',
            '2. Monitor progress': 'GET /job/{job_id} - Check status',
            '3. Check results': 'GET /status/{postal_code} - See final results'
        },
        'features': [
            'Background processing (no timeouts)',
            'Real-time progress tracking',
            'Job management',
            'Resume capability',
            'Multiple concurrent jobs'
        ]
    })

# Global app start time
app_start_time = datetime.now()

if __name__ == '__main__':
    print("Starting UberEats Scraper API with background processing...")
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True)