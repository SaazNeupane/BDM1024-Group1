# import the MRjob and MRStep for map reduce and defining the map reduce pipeline
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

#mapper class inheriting from MRJob
class CategoriesMapReduce(MRJob):
    
    #method steps overridden from MRJOB where steps for reducer and mapper is defined
    def steps(self):
        return [
            # configer the mapper and reducer here.
            MRStep(mapper = self.mapper_get_categories,
                   reducer = self.reducer_count_categories
                   )
        ]
    
    
    # defining our mapper function
    def mapper_get_categories(self, _ , line):
        """_summary_

        Args:
            line : the line object from the input data

        Yields:
            key, value: string, int as the output from the mapper
        """
        #reading the csv line
        reader = csv.reader([line], delimiter=',', quotechar='"')
        # getting the data
        row = next(reader)
        #storing the values
        # (id,	trans_date_trans_time,	cc_num,	merchant,	category) = row[0]
        (id,	trans_date_trans_time,	cc_num,	merchant,	category) = row[:5]
        # yielding the key value pair
        
        yield str(category),1
        
        
    # defining our reducer function
    def reducer_count_categories(self, key, values):
        """_summary_

        Args:
            values (tuple): key value pair from mapper

        Yields:
            key, value : key value pair returned from reducer.
        """
        yield key, sum(values)
        
    
        
# entry point        
if __name__ == "__main__":
    # Executing our map reduce class run method
    CategoriesMapReduce.run()