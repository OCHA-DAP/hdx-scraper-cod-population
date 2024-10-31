#!/usr/bin/python
"""cod-population scraper"""

import logging
import re
from typing import List, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class CODPopulation:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        temp_dir: str,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self.data = {
            "0": [],
            "1": [],
            "2": [],
        }

    def download_country_data(self, countries: List[str]) -> None:
        logger.info("Populating population table")
        for countryiso3 in countries:
            dataset_name = f"cod-ps-{countryiso3.lower()}"
            try:
                dataset = Dataset.read_from_hdx(dataset_name)
            except HDXError:
                logger.info(f"Can't read dataset for {countryiso3}")
                continue
            if not dataset:
                continue
            if dataset["archived"] or dataset.get("cod_level") is None:
                continue

            time_start = dataset.get_time_period(date_format="%Y-%m-%d")["startdate"]
            time_end = dataset.get_time_period(date_format="%Y-%m-%d")["enddate"]
            source = dataset["dataset_source"]
            for admin_level in self.data:
                # Find a csv resource for each admin level
                adm_resources = [
                    r
                    for r in dataset.get_resources()
                    if r.get_format() == "csv"
                    and re.match(f".*adm(in)?{admin_level}.*", r["name"], re.IGNORECASE)
                ]
                if len(adm_resources) == 0:
                    logger.warning(f"{countryiso3}: adm{admin_level} resource not found")
                    continue
                if len(adm_resources) > 1:
                    logger.warning(
                        f"{countryiso3}: more than one adm{admin_level} resource found"
                    )
                    continue
                resource = adm_resources[0]
                url = resource["url"]
                headers, rows = self._retriever.get_tabular_rows(
                    url,
                    dict_form=True,
                )
                # Find the correct p-code header and admin name headers
                if admin_level == "1" or admin_level == "2":
                    admin1_code_headers = _get_admin_headers(headers, admin_level)
                    admin1_name_headers = _get_admin_name_headers(headers, admin_level)
                    if len(admin1_code_headers) == 0:
                        logger.error(
                            f"{countryiso3}: adm{admin_level} code header not found"
                        )
                        continue
                    if len(admin1_name_headers) == 0:
                        logger.error(
                            f"{countryiso3}: adm{admin_level} name header not found"
                        )
                        continue

                if admin_level == "2":
                    admin2_code_headers = _get_admin_headers(headers, admin_level)
                    admin2_name_headers = _get_admin_name_headers(headers, admin_level)
                    if len(admin2_code_headers) == 0:
                        logger.error(
                            f"{countryiso3}: adm{admin_level} code header not found"
                        )
                        continue
                    if len(admin2_name_headers) == 0:
                        logger.error(
                            f"{countryiso3}: adm{admin_level} name header not found"
                        )
                        continue

                for row in rows:
                    if admin_level == "1" or admin_level == "2":
                        admin1_code = row[admin1_code_headers[0]]
                        admin1_name = row[admin1_name_headers[0]]
                    if admin_level == "2":
                        admin2_code = row[admin2_code_headers[0]]
                        admin2_name = row[admin2_name_headers[0]]

                    for header in headers:
                        if not re.match("^[FMT]_", header, re.IGNORECASE):
                            continue
                        population = row[header]
                        if "#" in population:
                            continue
                        if type(population) is str:
                            population = population.replace(",", "")
                        population = int(population)
                        gender, age_range = _get_gender_and_age_range(header)

                        population_values = {
                            "POPULATION_GROUP": header,
                            "GENDER": gender,
                            "AGE_RANGE": age_range,
                            "POPULATION": population,
                            "TIME_START": time_start,
                            "TIME_END": time_end,
                            "SOURCE": source,
                        }
                        admin_values = {
                            "ISO3": countryiso3,
                            "Country": Country.get_country_name_from_iso3(countryiso3),
                        }
                        if admin_level == "1" or admin_level == "2":
                            admin_values["ADM1_PCODE"] = admin1_code
                            admin_values["ADM1_NAME"] = admin1_name
                        if admin_level == "2":
                            admin_values["ADM2_PCODE"] = admin2_code
                            admin_values["ADM2_NAME"] = admin2_name
                        population_row = admin_values.update(population_values)
                        self.data[admin_level].append(population_row)

    def generate_dataset(self):
        dataset = Dataset()
        return dataset


def _get_admin_headers(headers: List[str], admin_level: str) -> List[str]:
    pattern = f"adm(in)?{admin_level}_?p?code"
    code_headers = [
        header for header in headers if re.match(pattern, header, re.IGNORECASE)
    ]
    return code_headers


def _get_admin_name_headers(headers: List[str], admin_level: str) -> List[str]:
    pattern = f"(adm(in)?{admin_level}(name)?_)((name$)|[a-z][a-z]$)"
    name_headers = [
        header for header in headers if re.match(pattern, header, re.IGNORECASE)
    ]
    return name_headers


def _get_gender_and_age_range(header: str) -> Tuple[str, str]:
    components = header.lower().split("_")
    gender = components[0]
    if gender == "t":
        gender = "all"
    if components[1] == "tl":
        age_range = "all"
        return gender, age_range
    if len(components[1:]) == 2:
        components[1] = str(int(components[1]))
        components[2] = str(int(components[2]))
    age_range = "-".join(components[1:])
    age_range = age_range.replace("plus", "+")
    return gender, age_range
