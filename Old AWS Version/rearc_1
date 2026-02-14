import boto3
import urllib.request
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil import parser

def import_file(base_url, target_folder, bucket_name, filename):
    #Download a single file and upload to S3
    try:
        file_url = base_url + filename
        s3_key = target_folder + filename
        
        #adding complexity so I can wait on learning how to do global installs for the request library
        req = urllib.request.Request(file_url, headers={'User-Agent': 'your-email@example.com'})
        file_response = urllib.request.urlopen(req)
        with urllib.request.urlopen(req) as response:
            file_content = response.read()
            last_modified = response.headers.get("Last-Modified")
        
        s3 = boto3.client('s3')
        try:
            s3_obj = s3.head_object(Bucket=bucket_name, Key=s3_key)
            s3_dt = s3_obj["LastModified"]
        except s3.exceptions.ClientError:
            s3_dt = None

        if parser.parse(last_modified) > s3_dt:
            s3.put_object(Bucket=bucket_name, Key=s3_key, Body=file_content)
            print(f"✓ Imported file: {s3_key}")
        else:
            print(f"✓ File already exists: {s3_key}")

        return filename
    except Exception as e:
        raise Exception(f"✗ Failed to import file:{filename}; {str(e)}")

def delete_file(bucket_name, target_folder, filename):
    #Delete a single file from S3
    try:
        s3_key = target_folder + filename

        s3 = boto3.client('s3')
        s3.delete_object(Bucket=bucket_name, Key=s3_key)
        print(f"✓ Deleted file: {s3_key}")
        return filename
    except Exception as e:
        raise Exception(f"✗ Failed to delete file:{s3_key}; {str(e)}")

def create_index_html_file (bucket_name, target_folder):
    #Create an index.html file in the S3 bucket
    try:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=target_folder)
        keys = [obj['Key'][len(target_folder):] for obj in response.get('Contents', [])]
        target_files = {filename for filename in keys if filename}

        html_content = "<html><body><ul>"
        for filename in target_files:
            html_content += f"<li><a href='{filename}'>{filename}</a></li>"
        html_content += "</ul></body></html>"

        s3.put_object(Bucket=bucket_name, Key=target_folder + 'index.html', Body=html_content, ContentType='text/html')
        print(f"✓ Created index.html file")
    except Exception as e:
        raise Exception(f"✗ Failed to create index.html file; {str(e)}")

def lambda_handler(event, context):
    try:
        #get configurations
        bucket_name = event.get('bucket_name', 'rivkasfirstawsbucket')
        base_url = event.get('base_url', 'https://download.bls.gov/pub/time.series/pr/')
        target_folder = event.get('target_folder', 'bls_data/raw/')

        #open link
        #adding complexity so I can wait on learning how to do global installs for the request library
        req = urllib.request.Request(base_url, headers={'User-Agent': 'your-email@example.com'})
        response = urllib.request.urlopen(req)
        html = response.read().decode('utf-8')
        #print(html)

        #get source filelist
        pattern = r'<a[^>]*href="[^"]*">(pr[^<]+)</a>'
        source_files = re.findall(pattern, html, flags=re.IGNORECASE)
        #print(source_files)
        source_files = {f for f in source_files if '://' not in f and not f.endswith('/')}
        
        # get target filelist
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=target_folder)
        keys = [obj['Key'][len(target_folder):] for obj in response.get('Contents', [])]
        target_files = {filename for filename in keys if filename}

        files_to_delete = {f for f in target_files - source_files if f != 'index.html'}   

        #import files
        import_errors = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(import_file, base_url, target_folder, bucket_name, f) 
                        for f in source_files]
            
            for future in as_completed(futures):
                try:
                    filename = future.result()
                except Exception as e:
                    print(str(e))
                    import_errors.append(str(e))
        if import_errors:
            raise Exception(f"✗ Failed to import files: {', '.join(import_errors)}")
        
        #delete files
        delete_errors = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(delete_file, bucket_name, target_folder, f) 
                        for f in files_to_delete]
            
            for future in as_completed(futures):
                try:
                    filename = future.result()
                except Exception as e:
                    print(str(e))
                    delete_errors.append(str(e))
        if delete_errors:
            raise Exception(f"✗ Failed to delete files: {', '.join(delete_errors)}")

        #create index file
        create_index_html_file(bucket_name, target_folder)

        return "See results at http://rivkasfirstawsbucket.s3-website.us-east-2.amazonaws.com/"
            
    except Exception as e:
        raise Exception(f"Error: {str(e)}")
        #Todo - better error handling
        
            

        
