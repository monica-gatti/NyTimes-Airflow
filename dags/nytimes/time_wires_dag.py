from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from nytimes.nytimes_common_package.utils import checkESIndexExists, createIndexEs
from nytimes.nytimes_common_package.model import Article, Author
from nytimes.nytimes_common_package.utils import dbPostgresGetEngine, logActivity, getNYTUrl, getUserAgent, getStringCurrentDate, ingestArticlesEs
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen, Request
from datetime import datetime
from time import sleep
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from psycopg2 import IntegrityError, errors
import json
import requests

webscrapingdag = DAG(
    dag_id='nytimes_webscraping_dag',
    description='This DAG perform a web scraping on NyTimes articles to get additional information like Authors and body lenght',
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

def getNYTimesArticlesListFromAPI():
    print("TimeWires - Started getting Articles List from API")
    UniqueViolation = errors.lookup('23505') 
    timeWireApiUrl = getNYTUrl(context="TIME_WIRES_CONTEXT_ALL")
    sectionData = requests.get(timeWireApiUrl).text
    sectionData = json.loads(sectionData)
    engine = dbPostgresGetEngine()
    Session = sessionmaker(bind=engine)
    s = Session()
    for result in sectionData["results"][1:200:1]:
        url = result["url"]
        try:
            article = Article( slug_id= result["slug_name"],article_date =result["created_date"],title = result["title"],section = result["section"],
                subsection = result["subsection"],url = url,apiInvokeDate = datetime.now(), scraped = "N")
            s.add(article)
            authors = result["byline"].replace("BY", "").replace("AND", ",").split(",")
            for item in authors:
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
            print(err)
            s.rollback()
            continue
        except Exception as err:
            print("generic error")
            raise
    s.close()
    print("TimeWires - Ended getting Articles List from API")


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
            s.add(article)
            s.commit()
            s.flush()
            sleep(2)
        except Exception as err:
            print(err)
            continue
    s.close()
    print("TimeWires - Ended Scraping Articles")
    print(n)

nytimes_es_create_index_task = PythonOperator(
    task_id='create_es_index',
    python_callable=createESIndex,
    dag=webscrapingdag
)
nytimes_import_articles_task = PythonOperator(
    task_id='extract_articles_data_from_API',
    python_callable=getNYTimesArticlesListFromAPI,
    dag=webscrapingdag
)
nytimes_scrape_articles_task = PythonOperator(
    task_id='extract_articles_data_from_web_scraping',
    python_callable=scrapeArticles,
    dag=webscrapingdag
)

nytimes_es_create_index_task >> nytimes_import_articles_task >> nytimes_scrape_articles_task