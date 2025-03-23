import json
from src.connector_class import HhAPIConnector
from src.db_handler_class import DbHandler
from src.db_manager import DBManager

with open("data/employers_list.json", "r") as f:
    data = json.load(f)
    employers = []
    for employer in data:
        employers.append({"id": int(employer["id"]), "name": employer["name"]})
ex1 = HhAPIConnector()
ex1.get_employer_info(employers)
ex1.get_openings("")
db_handler = DbHandler()
db_handler.create_db("test_db")
db_handler.create_tables()
db_handler.insert_values(ex1.employers_dict, ex1.openings_dict)

db_manager = DBManager(db_name="test_db")
db_manager.get_companies_and_vacancies_count("employers")
db_manager.get_all_vacancies("openings")
db_manager.get_avg_salary()
db_manager.get_vacancies_with_higher_salary()
keyword = str(input("Введите ключевое слово для поиска:  "))
db_manager.get_vacancies_with_keyword(keyword)
