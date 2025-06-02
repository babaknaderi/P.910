from abc import ABC, abstractmethod

class AdvanceDataCleaning(ABC):
    """
    Abstract class for advance data cleaning
    data: each submission data after normal data_cleaning method in result parser is called (marked by rejected, accepted, or accepted_and_use). 
    cfg is the congifuration file for result parser.
    """
    @abstractmethod
    def run(self, data, cfg):
        pass