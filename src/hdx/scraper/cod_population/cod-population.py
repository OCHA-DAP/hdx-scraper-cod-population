#!/usr/bin/python
"""cod-population scraper"""

import logging
from typing import List, Optional


from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class CODPopulation:

    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir

    def populate(self) -> None:
        logger.info("Populating population table")
        reader = Read.get_reader("hdx")
        datasets = reader.search_datasets(
            filename="*adm*",
            fq="name:cod-ps-*",
            configuration=self._configuration,
        )
        errors = set()
        warnings = set()
        for dataset in datasets:
            dataset_id = dataset["id"]
            dataset_name = dataset["name"]
            if dataset["archived"] or dataset.get("cod_level") is None:
                continue
            countryiso3 = dataset.get_location_iso3s()[0]
            if countryiso3 not in self._countryiso3s:
                continue
            self._metadata.add_dataset(dataset)
            time_period_start = dataset.get_time_period()["startdate"]
            time_period_end = dataset.get_time_period()["enddate"]
            for admin_level_int, admin_level in enumerate(["national", "adminone", "admintwo"]):
                # Find a csv resource for each admin level
                adm_resources = [
                    r
                    for r in dataset.get_resources()
                    if r.get_format() == "csv" and
                       re.match(f".*adm(in)?{admin_level_int}.*", r["name"], re.IGNORECASE)
                ]
                if len(adm_resources) == 0:
                    add_message(
                        warnings,
                        dataset_name,
                        f"{admin_level} resource not found",
                    )
                    continue
                if len(adm_resources) > 1:
                    add_message(
                        warnings,
                        dataset_name,
                        f"more than one {admin_level} resource found",
                    )
                resource = adm_resources[0]
                resource_id = resource["id"]
                self._metadata.add_resource(dataset_id, resource)
                url = resource["url"]
                headers, rows = reader.get_tabular_rows(
                    url,
                    dict_form=True,
                )
                # Find the correct p-code header
                admin_code_headers = _get_admin_header(headers, admin_level_int)
                if admin_level != "national" and len(admin_code_headers) != 1:
                    add_message(
                        errors,
                        dataset_name,
                        f"{admin_level} p-code header not found",
                    )
                    continue
                # Find the correct admin name headers
                admin1_name_headers, admin2_name_headers = _get_admin_name_headers(headers, admin_level_int)
                if admin_level == "adminone" and len(admin1_name_headers) != 1:
                    add_message(
                        errors,
                        dataset_name,
                        f"{admin_level} name header not found",
                    )
                    continue
                if admin_level == "admintwo" and len(admin2_name_headers) != 1:
                    add_message(
                        errors,
                        dataset_name,
                        f"{admin_level} name header not found",
                    )
                    continue
                for row in rows:
                    if admin_level == "national":
                        admin_code = countryiso3
                    else:
                        admin_code = row[admin_code_headers[0]]

                    if len(admin1_name_headers) == 0:
                        provider_admin1_name = ""
                    else:
                        provider_admin1_name = get_provider_name(row, admin1_name_headers[0])

                    if len(admin2_name_headers) == 0:
                        provider_admin2_name = ""
                    else:
                        provider_admin2_name = get_provider_name(row, admin2_name_headers[0])

                    # Look up the admin code, if not found assign to the national level
                    admin2_code = admins.get_admin2_code_based_on_level(
                        admin_code=admin_code, admin_level=admin_level
                    )
                    try:
                        admin2_ref = self._admins.admin2_data[admin2_code]
                    except KeyError:
                        add_missing_value_message(
                            errors,
                            dataset_name,
                            "p-code",
                            admin2_code,
                        )
                        admin2_code = admins.get_admin2_code_based_on_level(
                            admin_code=countryiso3, admin_level="national"
                        )
                        admin2_ref = self._admins.admin2_data[admin2_code]

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
                        min_age, max_age = get_min_and_max_age(age_range)
                        population_row = DBPopulation(
                            resource_hdx_id=resource_id,
                            admin2_ref=admin2_ref,
                            provider_admin1_name=provider_admin1_name,
                            provider_admin2_name=provider_admin2_name,
                            gender=gender,
                            age_range=age_range,
                            min_age=min_age,
                            max_age=max_age,
                            population=population,
                            reference_period_start=time_period_start,
                            reference_period_end=time_period_end,
                        )
                        self._session.add(population_row)
            self._session.commit()
        for warning in sorted(warnings):
            logger.warning(warning)
        for error in sorted(errors):
            logger.error(error)

    def _get_admin_header(headers: List[str], admin_level: int) -> List[str] | None:
        if admin_level == 0:
            return None
        code_headers = [
            header for header in headers if re.match(f"adm(in)?{admin_level}_?p?code", header, re.IGNORECASE)
        ]
        return code_headers

    def _get_admin_name_headers(headers: List[str], admin_level: int) -> Tuple[List, List]:
        if admin_level == 0:
            return [], []
        name_headers_1 = [
            header for header in headers if re.match(f"(adm(in)?1(name)?_)((name$)|[a-z][a-z]$)", header, re.IGNORECASE)
        ]
        if admin_level == 1:
            return name_headers_1, []
        name_headers_2 = [
            header for header in headers if re.match(f"(adm(in)?2(name)?_)((name$)|[a-z][a-z]$)", header, re.IGNORECASE)
        ]
        return name_headers_1, name_headers_2

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
