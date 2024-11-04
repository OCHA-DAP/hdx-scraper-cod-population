from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
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
        with temp_dir(
            "Test_cod_population",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                cod_population = CODPopulation(configuration, retriever, tempdir)
                cod_population.download_country_data("AFG")
                assert len(cod_population.data) == 2
                assert cod_population.data[0][0] == {
                    "ISO3": "AFG",
                    "Country": "Afghanistan",
                    "Population_group": "F_TL",
                    "Gender": "f",
                    "Age_range": "all",
                    "Population": 19844212,
                    "Date_start": "2021-01-01",
                    "Date_end": "2021-12-31",
                    "Source": "National Statistic and Information Authority (NSIA) "
                    "Afghanistan",
                }
                assert cod_population.data[1][0] == {
                    "ISO3": "AFG",
                    "Country": "Afghanistan",
                    "ADM1_PCODE": "AF17",
                    "ADM1_NAME": "Badakhshan",
                    "Population_group": "F_TL",
                    "Gender": "f",
                    "Age_range": "all",
                    "Population": 666004,
                    "Date_start": "2021-01-01",
                    "Date_end": "2021-12-31",
                    "Source": "National Statistic and Information Authority (NSIA) "
                    "Afghanistan",
                }
                assert cod_population.metadata == {
                    "countries": ["AFG"],
                    "date_end": ["2021-12-31"],
                    "date_start": ["2021-01-01"],
                }

                dataset = cod_population.generate_dataset()
                dataset.update_from_yaml(path=join(config_dir, "hdx_dataset_static.yaml"))
                assert dataset == {
                    "name": "cod-ps-global",
                    "title": "Global Subnational Population Statistics",
                    "groups": [{"name": "afg"}],
                    "dataset_date": "[2021-01-01T00:00:00 TO 2021-12-31T23:59:59]",
                    "tags": [
                        {
                            "name": "baseline population",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        }
                    ],
                    "license_id": "cc-by",
                    "methodology": "Other",
                    "methodology_other": "Compiled daily from individual COD population "
                    "statistics files.",
                    "caveats": "Please see the files for individual sources, and visit "
                    "each country page for more detailed methods.",
                    "dataset_source": "HDX",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                    "owner_org": "hdx",
                    "data_update_frequency": 1,
                    "notes": "Latest COD population statistics compiled at the admin "
                    "level. The CSV files contain subnational p-codes, their "
                    "corresponding administrative names, source organization, "
                    "and reference dates where available. These are constructed "
                    "from individual country level population files.",
                    "subnational": 1,
                }

                resources = dataset.get_resources()
                assert len(resources) == 2
