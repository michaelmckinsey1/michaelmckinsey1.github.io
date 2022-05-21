# Initial Idea/Source: (https://maoviola.medium.com/a-complete-guide-to-web-scraping-linkedin-job-postings-ad290fcaa97f)

import os
import sys
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from difflib import SequenceMatcher
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv
from distutils.util import strtobool

load_dotenv()  # take environment variables from .env.

DEBUG=False
PROCESS_OUTPUTS=True
CHROME_WEBDRIVER_MANAGER_VERSION = "99.0.4844.51" # This matches the version of chrome on the local machine.
SCRAPE_DATE = date.today()
if strtobool(os.getenv('PRODUCTION')):
    URL  = "https://www.linkedin.com/jobs/search?keywords=Computer%20Science&location=United%20States&locationId=&geoId=103644278&f_TPR=r3600"
elif not strtobool(os.getenv('PRODUCTION')):
    URL = "https://www.linkedin.com/jobs/search?keywords=Computer%20Science&location=College%20Station%2C%20Texas%2C%20United%20States&geoId=103723584&trk=public_jobs_jobs-search-bar_search-submit" # Linkedin job search url for: "Computer Science" in "College Station, Texas, United States"


def init_webdriver():
    if (PROCESS_OUTPUTS):
        print("Starting the webdriver...")
    # Setup Chromedriver
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized") # Maximize window to prevent LinkedIn from opening job in new window (Happens if window is too small).
    options.add_argument("incognito")
    web_driver = webdriver.Chrome(service=Service(ChromeDriverManager(CHROME_WEBDRIVER_MANAGER_VERSION).install()), options=options)
    return web_driver


def load_jobs(web_driver): # Not all jobs are loaded initially, so we need to load them manually.
    if (PROCESS_OUTPUTS):
        print("Loading webpage...")
    web_driver.get(URL)
    try: # Wait for the first card to load.
        WebDriverWait(web_driver, 10).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div/main/section[2]/ul/li[1]/div/a")))
    except:
        print("ERROR: Webpage did not load.")
        quit()
    num_jobs = "/html/body/div[1]/div/main/div/h1/span[1]"
    num_jobs_ele = web_driver.find_element(By.XPATH, num_jobs).get_attribute('innerText')
    num_jobs_ele = num_jobs_ele.replace(',', '')
    num_jobs_ele = num_jobs_ele.replace('+', '')
    temp_counter = 0
    if int(num_jobs_ele) > 25: # Below is not necessary and errors if jobs returned are less than 26.
        page_conclusion_xpath = "/html/body/div[1]/div/main/section[2]/div[2]" # XPath of the LinkedIn indicator that you have loaded all jobs.
        while (not web_driver.find_element(By.XPATH, page_conclusion_xpath).is_displayed()):
            web_driver.execute_script("window.scrollTo(0, 0);")
            web_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            try:
                web_driver.find_element(By.XPATH, '/html/body/div[1]/div/main/section[2]/button').click() # Sometimes a button appears to load more jobs.
            except:
                pass
            if temp_counter == 80:
                break
            temp_counter += 1


def scrape_metadata(jobs, s_chunk, e_chunk): # Scrape data that can be found on the page without additional loading.
    if (PROCESS_OUTPUTS):
        print("Scraping metadata...")
    titles = []
    employers = []
    locations = []
    post_dates = []
    scrape_dates = []
    urls = []
    for i in range(s_chunk, e_chunk):
        # Collect data from webpage.
        title = jobs[i].find_element(By.CSS_SELECTOR, 'h3').get_attribute('innerText')
        employer = jobs[i].find_element(By.CSS_SELECTOR, 'h4').get_attribute('innerText')
        location = jobs[i].find_element(By.CLASS_NAME, 'job-search-card__location').get_attribute('innerText')
        post_date = jobs[i].find_element(By.CSS_SELECTOR, 'div>div>time').get_attribute('datetime')
        url = jobs[i].find_element(By.CSS_SELECTOR, 'a').get_attribute('href')
        
        # Append data to correspoinding lists.
        titles.append(title)
        employers.append(employer)
        locations.append(location) 
        post_dates.append(post_date)
        scrape_dates.append(SCRAPE_DATE)
        urls.append(url)
    return [titles, employers, locations, post_dates, scrape_dates, urls]


def scrape_description(web_driver, jobs, s_chunk, e_chunk): # Scrape job descriptions from each job listing. Each job needs to be clicked on before description will load.
    if (PROCESS_OUTPUTS):
        print("Scraping descriptions...")
    jd = []
    prev_description = ""
    cur_description = ""
    jd_class = "div[class^='description__text']"
    for i in range(s_chunk, e_chunk):
        timeout_count = 0
        timeout_limit = 5
        sleep_time = 2.0
        acceptable_ratio = 0.8
        invalid_str_num_spaces = 10 # Weird behavior with invalid descriptions where there will be 28 spaces before the first character in the string.
        jobs[i].click() # Click on job card.
        time.sleep(sleep_time)
        while (SequenceMatcher(a=cur_description.replace(" ","").lower(),b=prev_description.replace(" ","").lower()).ratio() > acceptable_ratio or (cur_description[0:invalid_str_num_spaces].replace(" ","")=="")): # Sometimes the incorrect description is slightly different so we need a ratio.
            # Try to grab description.
            try:
                cur_description = web_driver.find_element(By.CSS_SELECTOR, jd_class).get_attribute('innerText') # Select element that contains description.
                if (SequenceMatcher(a=cur_description.replace(" ","").lower(),b=prev_description.replace(" ","").lower()).ratio() < acceptable_ratio) and (not cur_description[0:invalid_str_num_spaces].replace(" ","")==""):
                    break
            except:
                cur_description = f"Error: Couldn't grab job description at element {i}."
            # Try to refresh the loading
            jobs[i-1].click()
            time.sleep(sleep_time)
            jobs[i].click()
            time.sleep(sleep_time)
            # Check limit.
            timeout_count += 1
            if timeout_count >= timeout_limit:
                cur_description = f"Error: Job description grabber for element {i} timed out after {timeout_limit*sleep_time} seconds."
        prev_description = cur_description.replace(" ", "").lower()
        cur_description = cur_description.replace('\n',' ')
        jd.append(cur_description)
    return jd


def output_data_to_file(job_dataframe, filename):
    if (PROCESS_OUTPUTS):
        print ("Writing to file " + str(filename))
    job_dataframe.to_excel(filename, index = False)


def remove_error_entries(df):
    # Takes a pandas dataframe and removes the entries where the description wasn't grabbed correctly.
    indicator_string = "error"
    deletion_indexes = []
    for i in range(len(df)):
        description_beginning = df.iat[i, 6][:len(indicator_string)].lower()
        if description_beginning == indicator_string or description_beginning.replace(" ","")=="":
            deletion_indexes.append(i)
            if DEBUG:
                print(f"Row {i} removed.")
                print(str(df.iat[i, 6]))
    df=df.drop(deletion_indexes)
    if DEBUG:
        print(df)
    return df


def clear_old_jobs(connection_string):
    if PROCESS_OUTPUTS:
            print("Clearing old jobs...")
    engine = create_engine(connection_string)
    sql = text("DELETE FROM jobs WHERE scrape_date < now() - interval '2 weeks';")
    results = engine.execute(sql)
    if DEBUG:
        for record in results:
            print("\n", record)


def main():
    start = time.time()

    chunk_size = int(sys.argv[1])

    # Initialization.
    connection_string = ""
    if strtobool(os.getenv('PRODUCTION')):
        connection_string = f'postgresql://{os.getenv("USER")}:{os.getenv("PASSWORD")}@{os.getenv("HOST")}:{os.getenv("PORT")}/{os.getenv("DATABASE")}'
    elif not strtobool(os.getenv('PRODUCTION')):
        connection_string = 'postgresql://postgres:postgres@localhost:5432/test_db'
    else:
        print("Error: Invalid connection string.")
        exit()

    clear_old_jobs(connection_string)

    web_driver = init_webdriver() # Start the webdriver.
    load_jobs(web_driver) # Load the page.
    jobs = web_driver.find_element(By.CLASS_NAME, 'jobs-search__results-list').find_elements(By.TAG_NAME, 'li') # Get a list of jobs on the page.

    # Gather data.
    count_actual_jobs = 0
    chunk_counter = 0
    job_size = len(jobs)
    while chunk_counter < job_size:
        if chunk_size > job_size - chunk_counter:
            chunk_size = job_size - chunk_counter
        metadata = scrape_metadata(jobs, chunk_counter, chunk_counter+chunk_size)
        job_descriptions = scrape_description(web_driver, jobs, chunk_counter, chunk_counter+chunk_size)
        
        job_data = pd.DataFrame({ # Create dataframe out of lists.
            'title': metadata[0],
            'employer': metadata[1],
            'location': metadata[2],
            'post_date': metadata[3],
            'scrape_date': metadata[4],
            'url': metadata[5],
            'description': job_descriptions
        })

        if DEBUG:
            output_data_to_file(job_data, f'job_data{chunk_counter//chunk_size}.xlsx')

        cleaned_job_data = remove_error_entries(job_data)
        count_actual_jobs += len(cleaned_job_data)

        if PROCESS_OUTPUTS:
            print(f"Writing chunk {chunk_counter//chunk_size} to database...")
        engine = create_engine(connection_string)
        cleaned_job_data.to_sql(name='jobs', con=engine, if_exists='append', index=False)
        
        chunk_counter += chunk_size

    end = time.time() - start
    print(f"Program time: {end}s. {end/60}m. {end/(60*60)}h.")
    print(f"Jobs added to the database: {count_actual_jobs}")
    print(f"Total number of jobs: {str(len(jobs))}")


main()
