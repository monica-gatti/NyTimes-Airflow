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
    description='This DAG perform a web scraping on NyTimes articles',
    tags=['webscraping', 'nytimes'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(2),
    }
)
def getNYTimesArticles():
    print("TimeWiresAll_batch_import_start")
    sleep(10)
    print("sleeptime")
    UniqueViolation = errors.lookup('23505') 
    #logActivity(getStringCurrentDate() + "_timewiresall.log")

    timeWireApiUrl = getNYTUrl(context="TIME_WIRES_CONTEXT_ALL")
    sectionData = requests.get(timeWireApiUrl).text
    sectionData = json.loads(sectionData)
    engine = dbPostgresGetEngine()
    Session = sessionmaker(bind=engine)
    s = Session()
    for result in sectionData["results"][1:60:1]:
        available = "Y"
        url = result["url"]
        try:
            print("API invoke")
            req = Request(url, headers={'User-Agent': 'Chrome/39.0.2171.95'})
            print("urlopen")
            page = urlopen(req)
            print("bs")
            soup = bs(page, 'html.parser')
            print("body")
            body = ""
            for t in soup.findAll("p", attrs={"class":"css-at9mc1 evys1bk0"}):
                body = body + t.text
            result["body"] = body
            ingestArticlesEs(result["slug_name"], result["created_date"], body )
        except Exception as err:
            available = "N"
            print(err)
        authors = result["byline"].replace("BY", "").replace("AND", ",").split(",")
        try:
            article = Article( slug_id= result["slug_name"],article_date =result["created_date"],title = result["title"],section = result["section"],
                subsection = result["subsection"],url = url,webPageAvailability = available,apiInvokeDate = datetime.now())
            s.add(article)
            s.flush()
            s.commit()
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
            #print(f"Unexpected {err=}, {type(err)=}")
            raise
    s.close()
    # sleep(6)
    print("TimeWiresAll_batch_import_finished")

def createESIndex():
    print("Check index")
    if checkESIndexExists():
        print("index already present")
    else:
        createIndexEs()

    #decomment only to save all the json response in file
    #filename = "./output/all_" + getStringCurrentDate() + "_timewires.json" 
    #with open(filename, "w") as jsonFile:
    #    json.dump(sectionData, jsonFile)    

nytimes_import_articles_task = PythonOperator(
    task_id='import_articles',
    python_callable=getNYTimesArticles,
    dag=webscrapingdag
)


nytimes_es_create_index_task = PythonOperator(
    task_id='create_es_index',
    python_callable=createESIndex,
    dag=webscrapingdag
)

nytimes_es_create_index_task >> nytimes_import_articles_task