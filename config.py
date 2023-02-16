import logging

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

import yaml
import glob


class Config:
    """
        Reads config file from the specified path
    """

    def __init__(self, path_to_yaml_files: str):
        """
            Reads config file from the specified path
            Parameters
            -----------
                path_to_yaml_files: str
                    Config yaml file path 
        """
        self.path = path_to_yaml_files

    def get_config(self) -> list:
        """
            Returns list of configs read from the path
        """
        files = glob.glob(f"{self.path}/*.yaml")  # list of all .yaml files in a directory
        configs = []
        for file in files:
            logger.info(f"Reading config file {file}")

            with open(file, 'r') as stream:
                try:
                    configs.append(yaml.safe_load(stream))
                except yaml.YAMLError as exc:
                    logger.error(exc)
        return configs
