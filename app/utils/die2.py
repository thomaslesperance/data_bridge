from extract import Extractor
from transform import Transformer
from load import Loader


class DIE:

    def __init__(
        self,
        job_name,
        sources, 
        destinations, 
        jobs, 
        transform_fn = None, 
        email_msg = None
    ) ->  None:
        
        self.sources = sources
        self.destinations = destinations
        self.job = jobs[job_name]
        self.transform_fn = transform_fn
        self.email_msg = email_msg

        self.extractor = Extractor(self.sources, self.job["extract"])
        self.transformer = Transformer(self.transform_fn)
        self.loader = Loader(self.destinations, self.job["load"], self.email_msg)


    def run(self):
        data = self.extractor.extract()
        files = self.transformer.transform()
        response = self.loader.load()

        print(response)