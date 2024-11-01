#!/usr/bin/python
"""cod-population scraper"""

import logging
import re
from typing import List, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
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
        self.data = {}
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

            for admin_level in range(0, 5):
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
                    logger.error(
                        f"{countryiso3}: more than one adm{admin_level} resource found"
                    )
                    continue
                resource = adm_resources[0]
                url = resource["url"]
                headers, rows = self._retriever.get_tabular_rows(url)
                # Find the correct p-code header and admin name headers
                adm_code_headers = {}
                adm_name_headers = {}
                for adm_level in range(1, admin_level + 1):
                    code_headers = _get_code_headers(headers, admin_level)
                    name_headers = _get_name_headers(headers, admin_level)
                    if len(code_headers) == 0:
                        logger.error(
                            f"{countryiso3}: adm{adm_level} code header not found in adm{admin_level} resource"
                        )
                        continue
                    if len(name_headers) == 0:
                        logger.error(
                            f"{countryiso3}: adm{admin_level} name header not found in adm{admin_level} resource"
                        )
                        continue
                    adm_name_headers[admin_level] = name_headers[0]
                    adm_code_headers[admin_level] = code_headers[0]

                for row in rows:
                    if "#" in row[0]:
                        continue
                    adm_codes = {}
                    adm_names = {}
                    for adm_level in range(1, admin_level + 1):
                        adm_codes[adm_level] = row[
                            headers.index(adm_code_headers[adm_level])
                        ]
                        adm_names[adm_level] = row[
                            headers.index(adm_name_headers[adm_level])
                        ]

                    for header_i, header in enumerate(headers):
                        if not _match_population_header(header):
                            continue
                        population = row[header_i]
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
                        population_row = {
                            "ISO3": countryiso3,
                            "Country": Country.get_country_name_from_iso3(countryiso3),
                        }
                        for adm_level in range(1, admin_level + 1):
                            population_row[f"ADM{adm_level}_PCODE"] = adm_codes[adm_level]
                            population_row[f"ADM{adm_level}_NAME"] = adm_names[adm_level]
                        population_row.update(population_values)
                        self.data[admin_level].append(population_row)

    def generate_dataset(self):
        dataset = Dataset(
            {
                "name": self._configuration["dataset_name"],
                "title": self._configuration["dataset_title"],
            }
        )
        dataset.add_country_locations(self.metadata["countries"])
        date_start = min(self.metadata["date_start"])
        date_end = max(self.metadata["date_end"])
        dataset.set_time_period(date_start, date_end)
        dataset.add_tags(self._configuration["tags"])

        for admin_level, admin_data in self.data.items():
            dataset.generate_resource_from_iterable(
                headers=list(admin_data[0].keys()),
                iterable=admin_data,
                hxltags=self._configuration["hxl_tags"],
                folder=self._retriever.temp_dir,
                filename=f"admin{admin_level}_population.csv",
                resourcedata={
                    "name": f"admin{admin_level}_population.csv",
                    "description": " ",
                },
            )
        return dataset


def _get_code_headers(headers: List[str], admin_level: int) -> List[str]:
    pattern = f"adm(in)?{admin_level}_?p?code"
    code_headers = [
        header for header in headers if re.match(pattern, header, re.IGNORECASE)
    ]
    return code_headers


def _get_name_headers(headers: List[str], admin_level: int) -> List[str]:
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
