import requests
from rich import print
import csv
import os
from datetime import datetime

def get_data(page_number):
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-BD,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Origin': 'https://mediere.anofm.ro',
        'Referer': 'https://mediere.anofm.ro/app/module/mediere/jobs',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Cookie': 'mark3_sess=0eamhc0rb4rnlj55h48gu78q7m',
    }

    data = {
        "current": page_number,
        "rowCount": 100,
        "sort": {
            "created_at": "desc"
        }
    }
    
    try:
        response = requests.post('https://mediere.anofm.ro/api/entity/vw_public_job_posting', 
                                 headers=headers, 
                                 json=data)
        response.raise_for_status()  # Check for HTTP errors
        rows = response.json()['rows']
        
        if not rows:
            return []
        
        results = [clean_row(row) for row in rows]
        
        for row in results:
            print(row)
        
        return results
    
    except requests.exceptions.RequestException:
        return []

def clean_row(row):
    """Replace None values with empty strings and convert salary to float"""
    cleaned_row = {}
    for key, value in row.items():
        if value is None:
            cleaned_row[key] = ""
        elif key in ['minimum_salary', 'maximum_salary']:
            try:
                cleaned_row[key] = float(value)
            except (ValueError, TypeError):
                cleaned_row[key] = 0.0
        else:
            cleaned_row[key] = value
    return cleaned_row

def get_existing_jobs():
    """Read existing jobs from CSV file"""
    try:
        with open('job_postings.csv', 'r', encoding='utf-8') as file:
            return list(csv.DictReader(file))
    except FileNotFoundError:
        return []

def get_new_jobs_list():
    """Read jobs from new_jobs.csv"""
    try:
        with open('new_jobs.csv', 'r', encoding='utf-8') as file:
            return list(csv.DictReader(file))
    except FileNotFoundError:
        return []

def save_jobs_to_csv(jobs, filename):
    """Save jobs to CSV file"""
    if not jobs:
        return
        
    fieldnames = jobs[0].keys()
    with open(filename, 'w', encoding='utf-8', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(jobs)

def is_new_job(job, existing_jobs):
    """Check if job is not in existing jobs using the id field"""
    return not any(str(existing['id']) == str(job['id']) for existing in existing_jobs)

def cleanup_files():
    """Remove new_jobs.csv at start of each run"""
    try:
        if os.path.exists('new_jobs.csv'):
            os.remove('new_jobs.csv')
    except Exception:
        pass
    try:
        if os.path.exists('job_postings.csv'):
            os.remove('job_postings.csv')
    except Exception:
        pass

def main():
    # Clean up previous new_jobs.csv and job_postings.csv
    cleanup_files()
    
    # Get existing jobs
    existing_jobs = get_existing_jobs()
    
    # Scrape current jobs
    results = []
    page_number = 1
    while True:
        new_results = get_data(page_number=page_number)
        if not new_results:
            break
        results.extend(new_results)
        page_number += 1
        print(f'Getting jobs from page {page_number}')
        
        
    # Find new jobs by comparing with existing ones
    new_jobs = [job for job in results if is_new_job(job, existing_jobs)]
    
    if new_jobs:
        # Save only new jobs to new_jobs.csv
        save_jobs_to_csv(new_jobs, 'new_jobs.csv')
        
        # Add new jobs to existing jobs and save to job_postings.csv
        all_jobs = existing_jobs + new_jobs
        save_jobs_to_csv(all_jobs, 'job_postings.csv')

if __name__ == '__main__':
    main()
