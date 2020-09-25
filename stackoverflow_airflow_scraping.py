import requests
from bs4 import BeautifulSoup
import csv

csv_columns = ['question', 'question_link', 'answer', 'vote_count','question_date']
csv_file = "stackoverflow_airflow_questions.csv"
csv.register_dialect(
    'mydialect',
    delimiter=';',
    quotechar='"',
    doublequote=True,
    skipinitialspace=True,
    lineterminator='\r\n',
    quoting=csv.QUOTE_MINIMAL)

res = requests.get("https://stackoverflow.com/search?tab=Relevance&pagesize=50&q=airflow")

soup = BeautifulSoup(res.text, "html.parser")

questions_data = {
    "questions": []
}

questions = soup.select(".question-summary")

for que in questions:
    q = que.select_one('.question-hyperlink').getText().strip()

    q_link = "https://stackoverflow.com/" + que.select_one('.question-hyperlink').attrs['data-searchsession'].strip()

    vote_count = que.select_one('.vote-count-post').getText().strip()

    answer = que.select_one('.status').getText().strip()

    question_date = que.select_one('.relativetime').attrs['title'].strip()

    questions_data['questions'].append({
        "question": q,
        "question_link": q_link,
        "answer": answer,
        "vote_count": vote_count,
        "question_date": question_date
    })

try:
    with open(csv_file, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns, dialect='mydialect')
        writer.writeheader()
        for data in questions_data['questions']:
            print(data)
            writer.writerow(data)
except IOError:
    print("I/O error")
