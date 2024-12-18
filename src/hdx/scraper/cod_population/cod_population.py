#!/usr/bin/python
"""cod population scraper"""

import logging
import re
from typing import List, Tuple
from unicodedata import normalize

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dictandlist import dict_of_lists_add, dict_of_sets_add
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class CODPopulation:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        temp_dir: str,
        errors: ErrorsOnExit,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self.data = {}
        self.metadata = {}
        self.errors = errors
        self._nonmatching_headers = {}
        self._year_sources = {}

    def download_country_data(self, iso3: str) -> None:
        dataset_name = f"cod-ps-{iso3.lower()}"
        try:
            dataset = Dataset.read_from_hdx(dataset_name)
        except HDXError:
            logger.info(f"Can't read dataset for {iso3}")
            return
        if not dataset:
            return
        if dataset["archived"] or dataset.get("cod_level") is None:
            return

        logger.info(f"Downloading population data for {iso3}")
        year_end = int(dataset.get_time_period(date_format="%Y")["enddate_str"])
        source = dataset["dataset_source"]
        organization = dataset.get_organization()["display_name"]
        dict_of_lists_add(self.metadata, "countries", iso3)

        missing_levels = []
        for admin_level in range(0, 5):
            population_rows = []
            country_keys = set()
            duplicates = 0
            # Find a csv resource for each admin level
            adm_resources = [
                r
                for r in dataset.get_resources()
                if r.get_format() == "csv"
                and re.match(f".*adm(in)?{admin_level}.*", r["name"], re.IGNORECASE)
            ]
            if len(adm_resources) == 0:
                missing_levels.append(admin_level)
                continue
            if len(adm_resources) > 1:
                adm_resources = _select_latest_resource(adm_resources)
            if len(adm_resources) > 1:
                self.errors.add(f"{iso3}: more than one adm{admin_level} resource found")
                continue
            resource = adm_resources[0]
            url = resource["url"]
            encoding = self._configuration["encoding_exceptions"].get(
                resource["name"], "utf-8"
            )
            try:
                headers, rows = self._retriever.get_tabular_rows(url, encoding=encoding)
            except DownloadError:
                self.errors.add(f"{iso3}: download failed for {resource['name']}")
                continue
            # Find the correct p-code header and admin name headers
            adm_code_headers = {}
            adm_name_headers = {}
            for adm_level in range(1, admin_level + 1):
                code_headers = _get_code_headers(headers, adm_level)
                name_headers = _get_name_headers(
                    headers, adm_level, self._configuration["non_latin_alphabets"]
                )
                if len(code_headers) == 0:
                    self.errors.add(
                        f"{iso3}: adm{adm_level} code header not found in adm{admin_level}"
                    )
                else:
                    adm_code_headers[adm_level] = code_headers[0]
                if len(name_headers) == 0:
                    self.errors.add(
                        f"{iso3}: adm{adm_level} name header not found in adm{admin_level}"
                    )
                else:
                    adm_name_headers[adm_level] = name_headers[0]
            reference_year = self._configuration["reference_year_exceptions"].get(
                resource["name"]
            )
            resource_year = _get_resource_year(resource["name"])
            date_header = headers.index("year") if "year" in headers else None
            if reference_year:
                dict_of_sets_add(self._year_sources, iso3, "exception")
            for row in rows:
                row_non_null = [r for r in row if r]
                if "#" in row_non_null[0]:
                    continue
                if not reference_year:
                    if date_header is not None:
                        reference_year = int(row[date_header])
                        dict_of_sets_add(self._year_sources, iso3, "date header")
                    elif resource_year != -1:
                        reference_year = resource_year
                        dict_of_sets_add(self._year_sources, iso3, "resource name")
                    else:
                        reference_year = year_end
                        dict_of_sets_add(self._year_sources, iso3, "dataset date")
                dict_of_sets_add(self.metadata, "reference_year", reference_year)
                adm_codes = {}
                adm_names = {}
                for adm_level in range(1, admin_level + 1):
                    adm_code_header = adm_code_headers.get(adm_level)
                    if adm_code_header:
                        adm_codes[adm_level] = row[headers.index(adm_code_header)]
                    adm_name_header = adm_name_headers.get(adm_level)
                    if adm_name_header:
                        adm_name = row[headers.index(adm_name_header)]
                        if encoding == "latin-1":
                            try:
                                adm_name = normalize(
                                    "NFKD",
                                    adm_name.encode("latin-1", "ignore").decode("utf-8"),
                                )
                            except UnicodeDecodeError:
                                pass
                        adm_names[adm_level] = adm_name

                for header_i, header in enumerate(headers):
                    if not _match_population_header(header):
                        dict_of_sets_add(self._nonmatching_headers, iso3, header)
                        continue
                    population = row[header_i]
                    if population is None:
                        continue
                    if isinstance(population, str):
                        population = population.replace(",", "")
                    population = int(float(population))
                    gender, age_range = _get_gender_and_age_range(header)
                    min_age, max_age = _get_min_and_max_age(age_range)
                    if max_age and min_age and max_age < min_age:
                        self.errors.add(
                            f"{iso3}: adm{adm_level} has weird header {header}"
                        )
                        continue

                    population_values = {
                        "Population_group": header.upper(),
                        "Gender": gender,
                        "Age_range": age_range,
                        "Age_min": min_age,
                        "Age_max": max_age,
                        "Population": population,
                        "Reference_year": reference_year,
                        "Source": source,
                        "Contributor": organization,
                    }
                    population_row = {
                        "ISO3": iso3,
                        "Country": Country.get_country_name_from_iso3(iso3),
                    }
                    for adm_level in range(1, admin_level + 1):
                        population_row[f"ADM{adm_level}_PCODE"] = adm_codes.get(adm_level)
                        population_row[f"ADM{adm_level}_NAME"] = adm_names.get(adm_level)
                    population_row.update(population_values)
                    population_rows.append(population_row)

                    country_key = tuple(
                        value
                        for key, value in population_row.items()
                        if key
                        not in ["Population", "Reference_year", "Source", "Contributor"]
                    )
                    if country_key in country_keys:
                        duplicates += 1
                    country_keys.add(country_key)

            if duplicates > 0:
                self.errors.add(
                    f"{iso3}: {duplicates} duplicate values found in adm{admin_level}"
                )
                continue
            for population_row in population_rows:
                dict_of_lists_add(self.data, admin_level, population_row)

        missing_levels = _check_missing_levels(missing_levels)
        if len(missing_levels) > 0:
            error_message = f"{iso3} missing unexpected admin levels: {missing_levels}"
            if error_message not in self._configuration["known_errors"]:
                self.errors.add(error_message)

    def generate_dataset(self):
        dataset = Dataset(
            {
                "name": self._configuration["dataset_name"],
                "title": self._configuration["dataset_title"],
            }
        )
        dataset.add_country_locations(self.metadata["countries"])
        year_start = min(self.metadata["reference_year"])
        year_end = max(self.metadata["reference_year"])
        dataset.set_time_period_year_range(year_start, year_end)
        dataset.add_tags(self._configuration["tags"])
        dataset["cod_level"] = "cod-standard"

        for admin_level, admin_data in self.data.items():
            dataset.generate_resource_from_iterable(
                headers=list(admin_data[0].keys()),
                iterable=admin_data,
                hxltags=self._configuration["hxl_tags"],
                folder=self._retriever.temp_dir,
                filename=f"cod_population_admin{admin_level}.csv",
                resourcedata={
                    "name": f"cod_population_admin{admin_level}.csv",
                    "description": " ",
                },
                encoding="utf-8-sig",
            )
        return dataset


def _get_code_headers(headers: List[str], admin_level: int) -> List[str]:
    pattern = f"adm(in)?{admin_level}_?p?code"
    code_headers = [
        header for header in headers if re.match(pattern, header, re.IGNORECASE)
    ]
    return code_headers


def _get_name_headers(
    headers: List[str], admin_level: int, non_latin_alphabets: List[str]
) -> List[str]:
    pattern = f"(adm(in)?{admin_level}(name)?_?)((name$)|[a-z][a-z]$)"
    other_pattern = f"^name_?{admin_level}$"
    name_headers = [
        header
        for header in headers
        if re.match(pattern, header, re.IGNORECASE)
        or re.match(other_pattern, header, re.IGNORECASE)
    ]
    if len(name_headers) <= 1:
        return name_headers
    en_name_headers = [n for n in name_headers if n[-3:].lower() == "_en"]
    if len(en_name_headers) == 1:
        return en_name_headers
    latin_name_headers = [
        n
        for n in name_headers
        if n[-3] == "_" and n[-2:].lower() not in non_latin_alphabets
    ]
    if len(latin_name_headers) > 0:
        return latin_name_headers
    return name_headers


def _match_population_header(header: str) -> bool:
    total_pattern = "^[FMT]_TL$"
    range_pattern = "^[FMT]_[0-9]{1,3}_?[0-9]{1,3}$"
    plus_pattern = "^[FMT]_[0-9]{2,3}_?plus$"
    match = bool(
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
    # header format: f_00 or m_100
    if len(components) == 2 and len(components[1]) < 4:
        components.append(components[1])
    # header format: f_4045
    if len(components) == 2 and len(components[1]) == 4:
        components.append(components[1][-2:])
        components[1] = components[1][:2]
    if len(components[1:]) == 2:
        components[1] = str(int(components[1]))
        components[2] = str(int(components[2]))
    age_range = "-".join(components[1:])
    age_range = age_range.replace("plus", "+")
    return gender, age_range


def _get_min_and_max_age(age_range: str) -> (int | None, int | None):
    if age_range == "all" or age_range == "unknown":
        return None, None
    ages = age_range.split("-")
    if len(ages) == 2:
        # Format: 0-5
        min_age, max_age = int(ages[0]), int(ages[1])
    else:
        # Format: 80+
        min_age = int(age_range.replace("+", ""))
        max_age = None
    return min_age, max_age


def _check_missing_levels(missing_levels: List[int]) -> List[int]:
    expected_missing_levels = [i for i in range(5 - len(missing_levels), 5)]
    if missing_levels == expected_missing_levels:
        return []
    return missing_levels


def _select_latest_resource(adm_resources: List[Resource]) -> List[Resource]:
    adm_names = [adm_resource["name"] for adm_resource in adm_resources]
    year_matches = [_get_resource_year(name) for name in adm_names]
    year_matches = [int(y) for y in year_matches]
    max_index = year_matches.index(max(year_matches))
    if max_index == -1:
        return adm_resources
    adm_resources = [adm_resources[max_index]]
    return adm_resources


def _get_resource_year(resource_name: str) -> int:
    pattern = "(?<!\\d)2\\d{3}(?!\\d)"
    year_matches = re.findall(pattern, resource_name, re.IGNORECASE)
    if len(year_matches) == 0:
        return -1
    return int(year_matches[0])
