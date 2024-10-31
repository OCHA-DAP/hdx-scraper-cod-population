#!/usr/bin/python
"""cod-population scraper"""

import logging
import re
from typing import List, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date_range
from hdx.utilities.dictandlist import dict_of_lists_add
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
        self.metadata = {}

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

            date_start = dataset.get_time_period(date_format="%Y-%m-%d")["startdate_str"]
            date_end = dataset.get_time_period(date_format="%Y-%m-%d")["enddate_str"]
            source = dataset["dataset_source"]
            dict_of_lists_add(self.metadata, "countries", countryiso3)
            dict_of_lists_add(self.metadata, "date_start", date_start)
            dict_of_lists_add(self.metadata, "date_end", date_end)

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
                headers, rows = self._retriever.get_tabular_rows(url)
                # Find the correct p-code header and admin name headers
                if admin_level == "1" or admin_level == "2":
                    adm1_code_headers = _get_code_headers(headers, admin_level)
                    adm1_name_headers = _get_name_headers(headers, admin_level)
                    if len(adm1_code_headers) == 0:
                        logger.error(
                            f"{countryiso3}: adm{admin_level} code header not found"
                        )
                        continue
                    if len(adm1_name_headers) == 0:
                        logger.error(
                            f"{countryiso3}: adm{admin_level} name header not found"
                        )
                        continue
                    adm1_code_header = adm1_code_headers[0]
                    adm1_name_header = adm1_name_headers[0]

                if admin_level == "2":
                    adm2_code_headers = _get_code_headers(headers, admin_level)
                    adm2_name_headers = _get_name_headers(headers, admin_level)
                    if len(adm2_code_headers) == 0:
                        logger.error(
                            f"{countryiso3}: adm{admin_level} code header not found"
                        )
                        continue
                    if len(adm2_name_headers) == 0:
                        logger.error(
                            f"{countryiso3}: adm{admin_level} name header not found"
                        )
                        continue
                    adm2_code_header = adm2_code_headers[0]
                    adm2_name_header = adm2_name_headers[0]

                for row in rows:
                    if admin_level == "1" or admin_level == "2":
                        adm1_code = row[headers.index(adm1_code_header)]
                        adm1_name = row[headers.index(adm1_name_header)]
                    if admin_level == "2":
                        adm2_code = row[headers.index(adm2_code_header)]
                        adm2_name = row[headers.index(adm2_name_header)]

                    for header_i, header in enumerate(headers):
                        if not _match_population_header(header):
                            continue
                        population = row[header_i]
                        if "#" in population:
                            continue
                        if type(population) is str:
                            population = population.replace(",", "")
                        population = int(population)
                        gender, age_range = _get_gender_and_age_range(header)

                        population_values = {
                            "Population_group": header.upper(),
                            "Gender": gender,
                            "Age_range": age_range,
                            "Population": population,
                            "Date_start": date_start,
                            "Date_end": date_end,
                            "Source": source,
                        }
                        admin_values = {
                            "ISO3": countryiso3,
                            "Country": Country.get_country_name_from_iso3(countryiso3),
                        }
                        if admin_level == "1" or admin_level == "2":
                            admin_values["ADM1_PCODE"] = adm1_code
                            admin_values["ADM1_NAME"] = adm1_name
                        if admin_level == "2":
                            admin_values["ADM2_PCODE"] = adm2_code
                            admin_values["ADM2_NAME"] = adm2_name
                        population_row = admin_values.update(population_values)
                        self.data[admin_level].append(population_row)

    def generate_dataset(self):
        dataset = Dataset()
        return dataset


def _get_code_headers(headers: List[str], admin_level: str) -> List[str]:
    pattern = f"adm(in)?{admin_level}_?p?code"
    code_headers = [
        header for header in headers if re.match(pattern, header, re.IGNORECASE)
    ]
    return code_headers


def _get_name_headers(headers: List[str], admin_level: str) -> List[str]:
    pattern = f"(adm(in)?{admin_level}(name)?_)((name$)|[a-z][a-z]$)"
    name_headers = [
        header for header in headers if re.match(pattern, header, re.IGNORECASE)
    ]
    return name_headers


def _match_population_header(header: str) -> bool:
    total_pattern = "^[FMT]_TL$"
    range_pattern = "^[FMT]_[0-9]{2,3}_[0-9]{2,3}$"
    plus_pattern = "^[FMT]_[0-9]{2,3}_?plus$"
    match = (
        re.match(total_pattern, header, re.IGNORECASE)
        or re.match(range_pattern, header, re.IGNORECASE)
        or re.match(plus_pattern, header, re.IGNORECASE)
    )
    return match


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
