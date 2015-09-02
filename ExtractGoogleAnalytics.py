from __future__ import print_function

import luigi, get_ga
import MySQLdb
import time, argparse, csv
import json, datetime, time
# Important to note - luigi does not like parameters with underscores _ 

class ExtractGoogleAnalytics(luigi.Task):
    """args:
    Connection args:
        viewid = The view id for the google analytics account that you are extracting data from
        email = The email address for the api credential setup to pull for that google analytics account 
        p12file = Name of p12 file for api credential setup to pull for that google analytics account 
                    Save file and store under the p12 directory and rename it to remove spaces
    Query args:
        start = Start date of Query
        end = End date of Query
        metrics = metrics to include in Query
        dimensions = dimensions to include in Query
        filters = how you want to filter the Query
        sort = how you want to sort the Query
    returns:
        CSV file of the google query that is built
    """
    viewid = luigi.Parameter()
    email = luigi.Parameter()
    pfile = luigi.Parameter()
    metrics = luigi.Parameter()
    dimensions = luigi.Parameter(default='ga:date')
    filters = luigi.Parameter(default='ga:date==*')
    sort = luigi.Parameter(default='ga:date')

    one_day_ago = datetime.date.today() - datetime.timedelta(days=1)
    two_days_ago = datetime.date.today() - datetime.timedelta(days=2)

    start = luigi.Parameter(default=two_days_ago)
    end = luigi.Parameter(default=one_day_ago)

    def run(self):        
        file_location = 'p12/' + str(self.pfile)
        scope = ['https://www.googleapis.com/auth/analytics.readonly']
        service = get_ga.get_service('analytics', 'v3', scope, file_location, self.email)
        
        if self.filters == 'ga:date==*':
            ga_query = service.data().ga().get(
              ids='ga:' + self.viewid,
              start_date=str(self.start), 
              end_date=str(self.end), 
              metrics=str(self.metrics),
              dimensions=str(self.dimensions),
              sort=str(self.sort),
              start_index='1').execute()
        else:
            ga_query = service.data().ga().get(
              ids='ga:' + self.viewid,
              start_date=str(self.start), 
              end_date=str(self.end), 
              metrics=str(self.metrics),
              dimensions=str(self.dimensions),
              filters=str(self.filters),
              sort=str(self.sort),
              start_index='1').execute()
        filename = 'temp_%s_%s.csv' % (self.viewid, unicode(time.strftime('%Y%m%d')))
        
        for rows in ga_query:
            if rows == "rows":
                with open(filename, "wb") as f:
                    writer = csv.writer(f)
                    writer.writerows(ga_query[rows])
        f.close()

    def output(self):
        temp_name = 'temp_%s_%s.csv' % (self.viewid, unicode(time.strftime('%Y%m%d')))
        return luigi.LocalTarget(temp_name)

if __name__ == '__main__':
    luigi.run()