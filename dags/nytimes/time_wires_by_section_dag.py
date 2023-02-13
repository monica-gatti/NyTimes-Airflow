import csv
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from nytimes.nytimes_common_package.utils import checkESIndexExists, createIndexEs
from nytimes.nytimes_common_package.model import Article, Author
from nytimes.nytimes_common_package.utils import dbPostgresGetEngine, getNYTUrl, getStringCurrentDate, ingestArticlesEs
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen, Request
from datetime import datetime
from time import sleep
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from psycopg2 import IntegrityError, errors
import json
import requests
import boto3

webscrapingdagbysection = DAG(
    dag_id='nytimes_webscraping_by_section_dag',
    description='This DAG perform a web scraping on NyTimes articles by section to get additional information like Authors and body lenght',
    tags=['webscraping', 'nytimes'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(2),
    }
)

def createESIndex():
    print("Create index started")
    if checkESIndexExists():
        print("index already present")
    else:
        createIndexEs()
    print("Create index ended")


def getNYTimesArticlesListBySectionFromAPI():
    print("TimeWires - Started getting Articles List from API")
    UniqueViolation = errors.lookup('23505') 
    sectionListUrl = getNYTUrl(context="TIME_WIRES_SECTIONS_LIST")
    sectionListData = requests.get(sectionListUrl).text
    sectionListData = json.loads(sectionListData)
    engine = dbPostgresGetEngine()
    Session = sessionmaker(bind=engine)
    s = Session()
    for element in sectionListData["results"][2:3:1]:
        section = element["section"]
        timeWireApiUrl = getNYTUrl(context="TIME_WIRES_CONTEXT")
        sectionUrl = timeWireApiUrl % section
        sectionData = requests.get(sectionUrl).text
        sectionData = json.loads(sectionData)
        Session = sessionmaker(bind=engine)
        s = Session()
        for result in sectionData["results"]:
            url = result["url"]
            authors = result["byline"].replace("BY", "").replace("AND", ",").split(",")
            try:
                authors = result["byline"].replace("BY", "").replace("AND", "-")
                article = Article( slug_id= result["slug_name"],article_date =result["created_date"],title = result["title"],section = result["section"],
                subsection = result["subsection"], authors = authors, url = url, apiInvokeDate = datetime.now(), scraped = "N", exported= 'N')
                s.add(article)
                s.flush()
                s.commit()
                for item in authors.split("-"):
                    author = Author( slug_id = result["slug_name"] , fullname = item)
                    s.add(author)
                s.flush()
                s.commit()
            except UniqueViolation as uv:
                continue    
            except IntegrityError as e:
                assert isinstance(e.orig, UniqueViolation) 
                continue
            except SQLAlchemyError as err:
                print(str(err))
                s.rollback()
                continue
            except Exception as err:
                print(err)
                continue
    s.close()
    sleep(2)

def scrapeArticles():
    print("TimeWires - Started Scraping Articles")
    engine = dbPostgresGetEngine()
    Session = sessionmaker(bind=engine)
    s = Session()
    article_to_be_scraped = s.query(Article).filter(Article.scraped == "N").count()
    print("article_to_be_scraped", str(article_to_be_scraped))        
    n = 0
    for article in s.query(Article).filter(Article.scraped == "N").all():
        n+=1
        try:
            req = Request(article.url, headers={'User-Agent': 'Chrome/39.0.2171.95'})
            page = urlopen(req)
            soup = bs(page, 'html.parser')
            body = ""
            for t in soup.findAll("p", attrs={"class":"css-at9mc1 evys1bk0"}):
                body = body + t.text
            ingestArticlesEs(article.slug_id, article.article_date, body )
            article.scraped = "Y"
            article.word_count = len(body
            )
            s.add(article)
            s.commit()
            s.flush()
            sleep(1)
        except Exception as err:
            print(err)
            continue
    s.close()
    print("TimeWires - Ended Scraping Articles")
    print(n)

def createfile(**kwargs):
    engine = dbPostgresGetEngine()
    Session = sessionmaker(bind=engine)
    s = Session()
    article_filename = getStringCurrentDate() + '_article.csv'
    article_filepath = '/opt/airflow/export/' + article_filename

    with open(article_filepath, 'w') as f:
        out = csv.writer(f)
        out.writerow(['slug_id','article_date','title', 'section', 'subsection', 'word_count', 'authors', 'url'])
     
        for item in s.query(Article).filter(Article.exported == 'N').all():
            out.writerow([item.slug_id,item.article_date, item.title,item.section, item.subsection, item.word_count, item.authors, item.url])
            item.exported = 'Y'
            s.add(item)
            s.flush()
            s.commit()
    s.close()
    kwargs['ti'].xcom_push(key='articlefilename', value=article_filename)
    kwargs['ti'].xcom_push(key='articlefilepath', value=article_filepath)


def file_to_s3(**kwargs):
    #Creating Session With Boto3.
    session = boto3.Session(
    aws_access_key_id='AKIA4HQGPGUGRW3UN2FL',
    aws_secret_access_key='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'
    )
    articleFilename=kwargs['ti'].xcom_pull(key = 'articlefilename', task_ids='transform_articles_data_in_csv')
    articleFilepath=kwargs['ti'].xcom_pull(key = 'articlefilepath', task_ids='transform_articles_data_in_csv')
    #Creating S3 Resource From the Session.
    s3 = session.resource('s3')

    result = s3.Bucket('articlestorage').upload_file(articleFilepath, articleFilename )

    return (result)

nytimes_es_create_index_task = PythonOperator(
    task_id='setup',
    python_callable=createESIndex,
    dag=webscrapingdagbysection
)
nytimes_import_articles_task = PythonOperator(
    task_id='extract_articles_data_by_section_from_API',
    python_callable=getNYTimesArticlesListBySectionFromAPI,
    dag=webscrapingdagbysection
)
nytimes_scrape_articles_task = PythonOperator(
    task_id='extract_articles_data_from_web_scraping',
    python_callable=scrapeArticles,
    dag=webscrapingdagbysection
)

nytimes_transform_articles_task = PythonOperator(
    task_id='transform_articles_data_in_csv',
    python_callable=createfile,
    dag=webscrapingdagbysection
)
nytimes_load_articles_task = PythonOperator(
    task_id='load_data',
    python_callable=file_to_s3,
    dag=webscrapingdagbysection
)
nytimes_es_create_index_task >> nytimes_import_articles_task >> nytimes_scrape_articles_task >> nytimes_transform_articles_task >> nytimes_load_articles_task