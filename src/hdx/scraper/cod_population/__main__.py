#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import dirname, expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.cod_population.cod_population import CODPopulation

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-cod-population"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: cod-population"


def main(
    save: bool = True,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    with ErrorsOnExit() as errors_on_exit:
        with temp_dir(folder=_USER_AGENT_LOOKUP) as temp_folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=temp_folder,
                    saved_dir=_SAVED_DATA_DIR,
                    temp_dir=temp_folder,
                    save=save,
                    use_saved=use_saved,
                )
                configuration = Configuration.read()
                countryiso3s = [key for key in Country.countriesdata()["countries"]]

                # Steps to generate dataset
                cod_population = CODPopulation(
                    configuration, retriever, temp_folder, errors_on_exit
                )
                for iso3 in countryiso3s:
                    cod_population.download_country_data(iso3)
                dataset = cod_population.generate_dataset()
                dataset.update_from_yaml(
                    path=join(dirname(__file__), "config", "hdx_dataset_static.yaml")
                )
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    match_resource_order=False,
                    hxl_update=False,
                    updated_by_script=_UPDATED_BY_SCRIPT,
                )

                if len(errors_on_exit.errors) > 0:
                    errors = errors_on_exit.errors
                    errors = ["The following errors were found"] + sorted(errors)
                    with open("errors.txt", "w") as fp:
                        fp.writelines(_ + " | " for _ in errors)
                logger.info("Finished processing")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=join(
            dirname(__file__), "config", "project_configuration.yaml"
        ),
    )
