from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
import csv
import subprocess
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['i202310@nu.edu.pk'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'articles_ETL_pipeline',
    default_args=default_args,
    description='mlops a2 etl pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)

def fetch_page(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  
        return BeautifulSoup(response.content, 'html.parser')
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

# function to extract articles from landing page of bbc news
def scrap_bbc_articles(url):
    bbc_url = "https://www.bbc.com"
    soup = fetch_page(url)
    
    if not soup:
        return []

    articles_data = []
    articles = soup.find_all('div', attrs={"data-testid": "dundee-card"})
    articles += soup.find_all('div', attrs={"data-testid": "manchester-card"})

    for article in articles:
        link_tag = article.find('a', {'data-testid': 'internal-link'})
        if link_tag:
            link = bbc_url + link_tag['href']
            title = article.find('h2').get_text(strip=True) if article.find('h2') else 'No headline available'
            description = article.find('p', {'data-testid': 'card-description'}).get_text(strip=True) if article.find('p', {'data-testid': 'card-description'}) else 'No description available'
            
            articles_data.append([title, link, description])
    
    return articles_data

    

# Function to extract data from the article tags
def scrap_dawn_articles(url):
    # Send a GET request to the URL
    response = requests.get(url)
    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')
    # Find all article tags
    articles = soup.find_all('article')
    # Initialize a list to store the extracted data
    data = []
    # Iterate over each article
    for article in articles:
        st = article.find(class_="story__title")
        story_excerpt = article.find(class_='story__excerpt')
        if st:
            title = st.text.strip()
            link = article.find(class_="story__link")['href']
            description = ''
            if story_excerpt:
                description = story_excerpt.text.strip()

            # Append the extracted data to the list
            data.append((title, link, description))
    return data

# extract the articles from bbc and dawn landing pages
def extract_articles(**kwargs):
    dawn_url = 'https://www.dawn.com/'
    bbc_url = 'https://www.bbc.com/'
    dawn_data = scrap_dawn_articles(dawn_url)
    bbc_data = scrap_bbc_articles(bbc_url)

    return dawn_data + bbc_data

# cleaing the data i.e removing the articles which have no description
def clean_data(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_articles')
    cleaned_data = [row for row in extracted_data if row[2]]  
    return cleaned_data
  
# saving the articles in a csv file
def save_to_csv(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='clean_data')
    filename = '/home/kazim/Downloads/articles.csv'
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Title', 'Link', 'Description'])
        writer.writerows(data)
    return filename

def dvcpush():
    filename = '/home/kazim/Downloads/articles.csv'
    remote_url = 'gdrive://1bNBDL_4kcEueo-XYTWmKjr2Fosv-6QhQ'
    
    # Initialize DVC and set up the remote storage
    subprocess.run(['dvc', 'init', '-f'])
    subprocess.run(['dvc', 'remote', 'add', '-d', '-f', 'drive', remote_url], check=True)
    # Add the file to DVC and push it to the remotes
    subprocess.run(['dvc', 'add', filename], check=True)
    subprocess.run(['dvc', 'push'],check=True)


def gitpush():
    # Specify the filename and DVC remote URL
    filename = '/home/kazim/Downloads/articles.csv'
    git_repo_url = 'https://github.com/kazim-asif/mlops-a2.git'
    
    # Initialize Git repository
    subprocess.run(['git', 'init'], check=True)
    # Add all files to Git
    subprocess.run(['git', 'add', f'{filename}.dvc'], check=True)
    # Commit changes
    subprocess.run(['git', 'commit', '-m', 'Data added and pushed'], check=True)
    # Add remote
    subprocess.run(['git', 'remote', 'add', 'origin', git_repo_url], check=True)
    # Push to remote
    subprocess.run(['git', 'push', '-u', 'origin', 'main'], check=True)
    

def push_to_dvc_and_git(**kwargs):
    filename = '/home/kazim/Downloads/articles.csv' 
    dvcpush()
    gitpush()
    

# Task definitions
# task1 (extract articles)
extract_task = PythonOperator(
    task_id='extract_articles',
    python_callable=extract_articles,
    provide_context=True,
    dag=dag,
)

#task2 for cleaning the extracted articles
clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    provide_context=True,
    dag=dag,
)

#task3 for saving the articles
save_task = PythonOperator(
    task_id='save_to_csv',
    python_callable=save_to_csv,
    provide_context=True,
    dag=dag,
)

#task 4 to push the articles to dvc and git
push_task = PythonOperator(
    task_id='push_to_dvc_and_git',
    python_callable=push_to_dvc_and_git,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> clean_task >> save_task >> push_task
