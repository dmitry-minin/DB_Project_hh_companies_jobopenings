import json
from abc import ABC, abstractmethod
import requests


class BaseAPIConnector(ABC):

    @abstractmethod
    def get_openings(self, *args):
        """
        Abstract method for a get_data method for openings.
        """
        pass

    @abstractmethod
    def get_employer_info(self, *args):
        """
        Abstract method for a get_data method for employers.
        """
        pass



class HhAPIConnector(BaseAPIConnector):
    """
    A connector class for HH.ru API.
    Process get requests to HH API.
    """
    def __init__(self):
        """
        Initialize the HhAPIConnector.
        """
        self.endpoint_link_openings: str = "https://api.hh.ru/vacancies"
        self.endpoint_link_employer_info: str = "https://api.hh.ru/employers/"
        self.headers: dict = {"User-Agent": "HH-User-Agent"}
        self.body_params_o: dict = {"text": "", "employer_id": [], "page": 0, "per_page": 100, }
        self.body_params_e: dict = {"locale": "RU", "host": "hh.ru"}
        self.keys_needed_e: list = ["id", "name", "site_url", ["area", "name"], ["industries", "name"],
                                    "open_vacancies"]
        self.keys_needed_o: list = ["id", "name", ["area", "name"], "salary", ["employer", "id"], ["employer", "name"],
                                    ["snippet", "requirement"], ["snippet", "responsibility"]]
        self.openings_dict: list[dict] = []
        self.response_status_o: list = []
        self.response_status_e: list = []
        self.employers_dict: list[dict] = []

    def __repr__(self):
        """
        Return a string representation of the HhAPIConnector instance.
        """
        return (f"{self.__class__.__name__}({self.endpoint_link_openings}, {self.endpoint_link_employer_info},"
                f"{self.headers}, {self.body_params_o}, {self.body_params_e},{self.keys_needed_e},"
                f" {self.keys_needed_o}, {self.openings_dict}, {self.response_status_o}, {self.response_status_e}),"
                f"{self.employers_dict}")

    def get_openings(self, keyword: str, employer_id: int = "") -> None:
        """
        Make a GET request with User's keyword in the request and using Employer id's to the API endpoint and return
        the response data in python dict format.
        Result of the method is that obtained data recorded to the openings_dict attribute.
        Also processes possible errors
        """
        params = self.body_params_o.copy()
        params["text"] = keyword if keyword else ""
        employer_ids = [employer_id] if employer_id else [item["id"] for item in self.employers_dict]
        existing_ids = set()
        for employee_id in employer_ids:
            params["employer_id"] = employee_id
            page = 1
            while True:
                params["page"] = page
                try:
                    response = requests.get(self.endpoint_link_openings, headers=self.headers, params=params,
                                            timeout=30)
                    self.response_status_o.append({self.body_params_o["page"]: response.status_code})
                    response.raise_for_status()
                    data = response.json()

                    loaded_openings = data.get("items", [])
                    if not loaded_openings:
                        break
                    for item in loaded_openings:
                        ops_id = item.get("id")
                        if ops_id and ops_id not in existing_ids:
                            existing_ids.add(ops_id)
                            self.openings_dict.append(self.__extract_data_by_keys(item, self.keys_needed_o))
                    if params["page"] >= data.get("pages", 1) - 1:
                        break
                    page += 1

                except requests.exceptions.RequestException as e:
                    print(f"Ошибка при выполнении запроса: {e}")
                    break
                except ValueError:
                    print("Некорректный ответ API, формат не соответствует json")
                    break

    @staticmethod
    def __extract_data_by_keys(item: dict, keys: list) -> dict:
        """
        Extract necessary data from the provided data and keys.
        Result of the method is that obtained data recorded to the result dict attribute.
        """
        result = {}
        for key in keys:
            if isinstance(key, str):
                if key == "salary":
                    salary_data = item.get("salary")
                    if isinstance(salary_data, dict):
                        value = salary_data.get("to") or salary_data.get("from")
                    elif isinstance(salary_data, list):
                        value = next((s.get("to") or s.get("to") for s in salary_data
                                      if isinstance(s, dict)), None)
                    else:
                        value = None
                else:
                    value = item.get(key, None)
                result[key] = int(value) if isinstance(value, str) and value.isdigit() else value
            elif isinstance(key, list):
                if key[0] == "snippet" and isinstance(item.get(key[0]), dict):
                    result[key[1]] = item.get(key[0]).get(key[1], None)
                elif isinstance(item.get(key[0]), dict):
                    value = item.get(key[0]).get(key[1], None)
                    result["_".join(key)] = value if not str(value).isdigit() else int(value)
                elif isinstance(item.get(key[0]), list):
                    value = item.get(key[0])[0].get(key[1], None)
                    result["_".join(key)] = value if not str(value).isdigit() else int(value)

        return result

    def get_employer_info(self, employers_list: list[dict]):
        """
        Make a GET request with provided parameters (employers_list) to HH api endpoint.
        Result of the method is that obtained data recorded to the openings_dict attribute.
        Also processes possible errors
        """
        for employer in employers_list:
            endpoint = f"{self.endpoint_link_employer_info}{employer["id"]}"
            response = requests.get(endpoint, headers=self.headers, params=self.body_params_e, timeout=30)
            self.response_status_e.append({employer["id"]: response.status_code})
            loaded_employers_info = response.json()
            employers_filtered_by_keys = self.__extract_data_by_keys(loaded_employers_info, self.keys_needed_e)
            self.employers_dict.append(employers_filtered_by_keys)


if __name__ == "__main__":
    # ex = HhAPIConnector()
    # ex.get_openings("Python")
    # print(ex.openings_dict)
    with open("../data/employers_list.json", "r") as f:
        employers = json.load(f)
    ex = HhAPIConnector()
    ex.get_employer_info([{"id": 1740}, {"id": 3529}])

    ex.get_openings("")
