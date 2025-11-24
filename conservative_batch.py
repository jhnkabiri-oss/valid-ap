#!/usr/bin/env python3
"""
PeopleDataLabs Conservative Batch Email Lookup Tool
Script untuk melakukan lookup batch dengan rate limiting yang lebih konservatif
"""

import requests
import json
import re
import time
from datetime import datetime
from typing import Dict, Any, Optional

class PeopleDataLabsLookup:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.peopledatalabs.com/v5/person/identify"
        self.headers = {
            "X-Api-Key": self.api_key,
            "Content-Type": "application/json"
        }
    
    def search_by_email(self, email: str, show_rate_info: bool = False) -> Optional[Dict[Any, Any]]:
        """Cari person berdasarkan email"""
        try:
            response = requests.get(
                self.base_url, 
                headers=self.headers, 
                params={"email": email},
                timeout=30
            )
            
            # Show rate limit info from headers
            if show_rate_info and response.headers:
                rate_limit = response.headers.get('X-RateLimit-Limit', 'Unknown')
                rate_remaining = response.headers.get('X-RateLimit-Remaining', 'Unknown')
                rate_reset = response.headers.get('X-RateLimit-Reset', 'Unknown')
                print(f"   📊 Rate Info - Limit: {rate_limit}, Remaining: {rate_remaining}, Reset: {rate_reset}")
            
            if response.status_code == 429:
                print(f"   ⏳ Rate limit hit for {email}, skipping...")
                return "RATE_LIMITED"
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error untuk {email}: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ JSON Error untuk {email}: {e}")
            return None

def calculate_age(birth_year):
    """Hitung umur berdasarkan tahun lahir"""
    if birth_year:
        current_year = datetime.now().year
        return current_year - birth_year
    return None

def format_phone(phone):
    """Format nomor telepon"""
    if not phone:
        return ""
    
    # Remove + and other characters
    clean_phone = phone.replace("+1", "").replace("+", "").replace("-", "").replace("(", "").replace(")", "").replace(" ", "")
    
    # Format as (XXX) XXX-XXXX if 10 digits
    if len(clean_phone) == 10:
        return f"({clean_phone[:3]}) {clean_phone[3:6]}-{clean_phone[6:]}"
    return phone

def extract_person_info(api_response):
    """Extract informasi dari API response"""
    if not api_response or 'matches' not in api_response or not api_response['matches']:
        return None
    
    person = api_response['matches'][0]['data']
    
    # Extract basic info
    full_name = person.get('full_name', 'N/A')
    age = calculate_age(person.get('birth_year'))
    birth_date = person.get('birth_date', 'N/A')
    
    # Extract emails
    emails = []
    if person.get('emails'):
        for email_obj in person['emails']:
            emails.append(email_obj.get('address', ''))
    
    # Extract phone numbers
    phones = person.get('phone_numbers', [])
    mobile_phone = person.get('mobile_phone', '')
    
    # Extract location/address
    address_parts = []
    if person.get('location_name'):
        address_parts.append(person['location_name'])
    
    return {
        'full_name': full_name,
        'emails': emails,
        'phones': phones,
        'mobile_phone': mobile_phone,
        'address': address_parts[0] if address_parts else 'N/A',
        'age': age,
        'birth_date': birth_date
    }

def format_output(person_info, search_email):
    """Format output sesuai template yang diminta"""
    if not person_info:
        return f"Name: No data found\nEmail: {search_email}\nPhone Number: Not available\nAddress: Not available\nFull Name: No data found\nAge: Not specified\nDOB: Not specified"
    
    output_lines = []
    
    # Basic information format
    output_lines.append(f"Name: {person_info['full_name']}")
    output_lines.append(f"Email: {search_email}")
    
    # Phone dengan flag (ambil phone pertama)
    if person_info['phones']:
        formatted_phone = format_phone(person_info['phones'][0])
        output_lines.append(f"Phone Number: 🇺🇸{formatted_phone}")
    elif person_info['mobile_phone']:
        formatted_phone = format_phone(person_info['mobile_phone'])
        output_lines.append(f"Phone Number: 🇺🇸{formatted_phone}")
    else:
        output_lines.append("Phone Number: Not available")
    
    # Address
    if person_info['address'] != 'N/A':
        output_lines.append(f"Address: {person_info['address']}")
    else:
        output_lines.append("Address: Not available")
    
    # Other emails (tanpa label)
    for email in person_info['emails']:
        if email != search_email:  # Jangan ulangi search email
            output_lines.append(email)
    
    # Detailed info section
    output_lines.append(f"Full Name: {person_info['full_name']}")
    
    if person_info['age']:
        output_lines.append(f"Age: {person_info['age']}")
    else:
        output_lines.append("Age: Not specified")
    
    # Additional phone numbers (tanpa emoji)
    if person_info['phones']:
        for i, phone in enumerate(person_info['phones']):
            if i == 0:  # Skip first phone (already shown above)
                continue
            formatted_phone = format_phone(phone)
            output_lines.append(f"Phone Number: {formatted_phone}")
    
    # Birth date
    if person_info['birth_date'] != 'N/A':
        try:
            # Try to format birth date as MM/DD/YYYY
            birth_str = str(person_info['birth_date'])
            if len(birth_str) >= 10:  # YYYY-MM-DD format
                year = birth_str[:4]
                month = birth_str[5:7]
                day = birth_str[8:10]
                formatted_date = f"{month}/{day}/{year}"
                output_lines.append(f"DOB: {formatted_date}")
            else:
                output_lines.append(f"DOB: {person_info['birth_date']}")
        except:
            output_lines.append(f"DOB: {person_info['birth_date']}")
    else:
        output_lines.append("DOB: Not specified")
    
    return "\n".join(output_lines)

def parse_emails_from_list(filename="list.txt"):
    """Extract emails dari file list.txt"""
    emails = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Try multiple regex patterns to handle different formats
            # Pattern 1: Format dengan emoji 📧 : email@domain.com
            email_pattern_1 = r'📧 : ([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
            matches_1 = re.findall(email_pattern_1, content)
            
            # Pattern 2: Email langsung tanpa emoji (format baru)
            email_pattern_2 = r'^([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$'
            matches_2 = re.findall(email_pattern_2, content, re.MULTILINE)
            
            # Pattern 3: General email pattern untuk menangkap semua email
            email_pattern_3 = r'\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b'
            matches_3 = re.findall(email_pattern_3, content)
            
            # Combine all matches and remove duplicates
            all_matches = matches_1 + matches_2 + matches_3
            emails = list(set(all_matches))  # Remove duplicates
            
        print(f"✅ Found {len(emails)} unique emails in {filename}")
        return emails
    except FileNotFoundError:
        print(f"❌ File {filename} not found!")
        return []
    except Exception as e:
        print(f"❌ Error reading {filename}: {e}")
        return []

def append_to_valid_file(formatted_output, filename="valid.txt"):
    """Append hasil ke valid.txt secara real-time"""
    try:
        with open(filename, 'a', encoding='utf-8') as f:
            # Use consistent separators and add one blank line after each block
            f.write("=" * 50 + "\n")
            f.write(formatted_output + "\n")
            f.write("=" * 50 + "\n\n")
    except Exception as e:
        print(f"❌ Error writing to {filename}: {e}")


def append_to_lookup_file(formatted_output):
    """Append lookup results into lookup.txt (convenience wrapper)
    This keeps the default behaviour of valid.txt unchanged but provides a clear
    function to call when saving lookup-specific results.
    """
    append_to_valid_file(formatted_output, filename="lookup.txt")

def main():
    """Main function untuk batch processing"""
    print("PeopleDataLabs Conservative Batch Email Lookup Tool")
    print("=" * 60)
    
    api_key = "af4f887ea97581b4bd22d61bc2be713116e27753f44897d107e2b02d43297601"
    lookup = PeopleDataLabsLookup(api_key)
    
    # Parse emails from list.txt
    emails = parse_emails_from_list("list.txt")
    
    if not emails:
        print("❌ No emails found or error reading list.txt")
        return
    
    # Clear valid.txt file
    try:
        with open("valid.txt", 'w', encoding='utf-8') as f:
            f.write("")  # Clear file
        print("✅ Cleared valid.txt file")
    except Exception as e:
        print(f"❌ Error clearing valid.txt: {e}")
    
    print(f"\n🚀 Starting conservative batch lookup for {len(emails)} emails...")
    print("Results will be saved to valid.txt in real-time...")
    print("Using 10-second delay between requests (Rate limit: 10/minute)...\n")
    
    successful = 0
    failed = 0
    rate_limited = 0
    
    for i, email in enumerate(emails, 1):
        print(f"[{i}/{len(emails)}] Processing: {email}")
        
        # Search - show rate info on first request
        show_rate_info = (i == 1)
        result = lookup.search_by_email(email, show_rate_info)
        
        if result == "RATE_LIMITED":
            rate_limited += 1
            print(f"   ⚠️  Rate limited, skipping {email}")
            continue
        elif result:
            person_info = extract_person_info(result)
            formatted_output = format_output(person_info, email)
            
            # Append to valid.txt immediately
            append_to_valid_file(formatted_output)
            
            if person_info:
                successful += 1
                print(f"   ✅ Found data for {email} -> saved to valid.txt")
            else:
                failed += 1
                print(f"   ⚠️  No data found for {email} -> saved to valid.txt")
        else:
            # API error, still save with no data format
            formatted_output = format_output(None, email)
            append_to_valid_file(formatted_output)
            failed += 1
            print(f"   ❌ API error for {email} -> saved to valid.txt")
        
        # Conservative rate limiting - wait 10 seconds between requests (safe for 10 req/min limit)
        if i < len(emails):
            print(f"   ⏳ Waiting 10 seconds before next request...")
            time.sleep(10)
    
    # Summary
    print(f"\n" + "=" * 60)
    print("BATCH PROCESSING SUMMARY")
    print("=" * 60)
    print(f"Total emails processed: {len(emails) - rate_limited}")
    print(f"Successful lookups: {successful}")
    print(f"Failed/No data: {failed}")
    print(f"Rate limited (skipped): {rate_limited}")
    print(f"Results saved to: valid.txt")
    print("=" * 60)

if __name__ == "__main__":
    main()