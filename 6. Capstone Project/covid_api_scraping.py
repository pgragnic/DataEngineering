import requests
import pandas as pd
import os
import time
from datetime import datetime

def github_api(user, repo, url, urlraw, output_path):

    """
    Description: This function scraps COVID csv files from Github

    Arguments:
        user: github user
        repo: github repository
        url: url of the github repository (api)
        urlraw: url of the raw data of the github repository

    Returns:
        None
    """

    r = requests.get(url)
    res = r.json()
    i = 0

    start_time = time.time()

    for file in res["tree"]:
        pathToCheck = file["path"]
        if i == 10000:
            break
        if pathToCheck.endswith('.csv') \
                and pathToCheck.find("data/" + output_path)!=-1 \
                and pathToCheck.find('_us')==-1 \
                and not os.path.isfile("data/" + file["path"]):
                #r = requests.get(urlRaw + file["path"])
                r = requests.head(urlRaw + file["path"])
                if int(r.headers['Content-Length']) > 50000:
                    r = requests.get(urlRaw + file["path"])
                    open("data/" + file["path"], 'wb').write(r.content)
                    i += 1
                if i%10 == 0 and i > 0:
                    print(f'{i} files downloaded in {(time.time() - start_time)} seconds')
                    start_time = time.time()

user = "CSSEGISandData"
repo = "COVID-19"
url = "https://api.github.com/repos/{}/{}/git/trees/master?recursive=1".format(user, repo)
urlRaw = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/"
output_path = "csse_covid_19_daily_reports"


def main():
    github_api(user, repo, url, urlRaw, output_path)

if __name__ == "__main__":
    main()