from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.cod_population.cod_population import CODPopulation


class TestCODPopulation:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def read_dataset(self, monkeypatch):
        def read_from_hdx(dataset_name):
            return Dataset.load_from_json(
                join(
                    "tests",
                    "fixtures",
                    "input",
                    f"dataset-{dataset_name}.json",
                )
            )

        monkeypatch.setattr(Dataset, "read_from_hdx", staticmethod(read_from_hdx))

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "cod_population", "config")

    def test_cod_population(
        self, configuration, read_dataset, fixtures_dir, input_dir, config_dir
    ):
        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "Test_cod_population",
                delete_on_success=True,
                delete_on_failure=False,
            ) as temp_folder:
                with Download(user_agent="test") as downloader:
                    retriever = Retrieve(
                        downloader=downloader,
                        fallback_dir=temp_folder,
                        saved_dir=input_dir,
                        temp_dir=temp_folder,
                        save=False,
                        use_saved=True,
                    )
                    cod_population = CODPopulation(
                        configuration, retriever, temp_folder, error_handler
                    )
                    cod_population.download_country_data("CAF")
                    cod_population.download_country_data("COD")
                    assert len(cod_population.data) == 4
                    assert cod_population.data[0][0] == {
                        "ISO3": "COD",
                        "has_hrp": "Y",
                        "in_gho": "Y",
                        "admin_level": 0,
                        "Country": "Democratic Republic of the Congo",
                        "ADM1_PCODE": None,
                        "ADM1_NAME": None,
                        "ADM2_PCODE": None,
                        "ADM2_NAME": None,
                        "ADM3_PCODE": None,
                        "ADM3_NAME": None,
                        "ADM4_PCODE": None,
                        "ADM4_NAME": None,
                        "Population_group": "F_00_04",
                        "Gender": "f",
                        "Age_range": "0-4",
                        "Age_min": 0,
                        "Age_max": 4,
                        "Population": 9853646,
                        "Reference_year": 2020,
                        "Source": "Health Zone population statistics developed by the DRC IM "
                        "Working Group",
                        "Contributor": "OCHA Democratic Republic of the Congo (DRC)",
                        "dataset_hdx_id": "d1160fa9-1d58-4f96-9df5-edbff2e80895",
                        "resource_hdx_id": "fa85e725-30c5-4f9a-aefc-2e8db0db36fa",
                    }
                    assert cod_population.data[1][0] == {
                        "ISO3": "CAF",
                        "has_hrp": "Y",
                        "in_gho": "Y",
                        "admin_level": 1,
                        "Country": "Central African Republic",
                        "ADM1_PCODE": "CF11",
                        "ADM1_NAME": "Ombella M'Poko",
                        "ADM2_PCODE": None,
                        "ADM2_NAME": None,
                        "ADM3_PCODE": None,
                        "ADM3_NAME": None,
                        "ADM4_PCODE": None,
                        "ADM4_NAME": None,
                        "Population_group": "T_TL",
                        "Gender": "all",
                        "Age_range": "all",
                        "Age_min": None,
                        "Age_max": None,
                        "Population": 448465,
                        "Reference_year": 2015,
                        "Source": "General Census of Population and Housing, Census Office "
                        "Central African Republic",
                        "Contributor": "OCHA Central African Republic",
                        "dataset_hdx_id": "d3600c4b-d93d-4ed0-b7b1-359a060b916a",
                        "resource_hdx_id": "58403047-f30e-4719-94b0-8d6f7e0f6942",
                    }
                    assert cod_population.data[2][0] == {
                        "ISO3": "CAF",
                        "has_hrp": "Y",
                        "in_gho": "Y",
                        "admin_level": 2,
                        "Country": "Central African Republic",
                        "ADM1_PCODE": "CF22",
                        "ADM1_NAME": "Nana Mambéré",
                        "ADM2_PCODE": "CF224",
                        "ADM2_NAME": "Abba",
                        "ADM3_PCODE": None,
                        "ADM3_NAME": None,
                        "ADM4_PCODE": None,
                        "ADM4_NAME": None,
                        "Population_group": "T_TL",
                        "Gender": "all",
                        "Age_range": "all",
                        "Age_min": None,
                        "Age_max": None,
                        "Population": 28016,
                        "Reference_year": 2015,
                        "Source": "General Census of Population and Housing, Census "
                        "Office Central African Republic",
                        "Contributor": "OCHA Central African Republic",
                        "dataset_hdx_id": "d3600c4b-d93d-4ed0-b7b1-359a060b916a",
                        "resource_hdx_id": "2b74b781-c889-4cb9-9624-418577809c1e",
                    }
                    assert cod_population.data[3][0] == {
                        "ISO3": "CAF",
                        "has_hrp": "Y",
                        "in_gho": "Y",
                        "admin_level": 3,
                        "Country": "Central African Republic",
                        "ADM1_PCODE": "CF11",
                        "ADM1_NAME": "Ombella M'Poko",
                        "ADM2_PCODE": "CF111",
                        "ADM2_NAME": "Bimbo",
                        "ADM3_PCODE": "CF1111",
                        "ADM3_NAME": "Bimbo",
                        "ADM4_PCODE": None,
                        "ADM4_NAME": None,
                        "Population_group": "T_TL",
                        "Gender": "all",
                        "Age_range": "all",
                        "Age_min": None,
                        "Age_max": None,
                        "Population": 276042,
                        "Reference_year": 2015,
                        "Source": "General Census of Population and Housing, Census Office "
                        "Central African Republic",
                        "Contributor": "OCHA Central African Republic",
                        "dataset_hdx_id": "d3600c4b-d93d-4ed0-b7b1-359a060b916a",
                        "resource_hdx_id": "17c5c468-fb12-4114-8286-3feb2069ab0a",
                    }
                    assert cod_population.metadata == {
                        "countries": ["CAF", "COD"],
                        "reference_year": {2020, 2015},
                    }

                    dataset = cod_population.generate_dataset()
                    assert dataset == {
                        "name": "cod-ps-global",
                        "title": "OCHA Global Subnational Population Statistics",
                        "groups": [{"name": "caf"}, {"name": "cod"}],
                        "dataset_date": "[2015-01-01T00:00:00 TO 2020-12-31T23:59:59]",
                        "cod_level": "cod-standard",
                        "tags": [
                            {
                                "name": "baseline population",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "sex and age disaggregated data-sadd",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                    }

                    resources = dataset.get_resources()
                    assert len(resources) == 4
                    for i in range(0, 4):
                        assert_files_same(
                            join("tests", "fixtures", f"cod_population_admin{i}.csv"),
                            join(temp_folder, f"cod_population_admin{i}.csv"),
                        )

                    hapi_dataset = cod_population.generate_hapi_dataset()
                    assert hapi_dataset == {
                        "name": "hdx-hapi-population",
                        "title": "HDX HAPI - Geography & Infrastructure: Baseline Population",
                        "groups": [{"name": "caf"}, {"name": "cod"}],
                        "dataset_date": "[2015-01-01T00:00:00 TO 2020-12-31T23:59:59]",
                        "tags": [
                            {
                                "name": "baseline population",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "sex and age disaggregated data-sadd",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                    }
                    assert hapi_dataset.get_resources() == [
                        {
                            "name": "Global Geography & Infrastructure: Baseline Population "
                            "(HRP countries)",
                            "description": "Baseline Population data from HDX HAPI for HRP "
                            "countries, please see [the documentation](https://hdx-hapi."
                            "readthedocs.io/en/latest/data_usage_guides/geography_and_"
                            "infrastructure/#baseline-population) for more information",
                            "format": "csv",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        }
                    ]
                    assert_files_same(
                        join("tests", "fixtures", "hdx_hapi_population_global_hrp.csv"),
                        join(temp_folder, "hdx_hapi_population_global_hrp.csv"),
                    )
